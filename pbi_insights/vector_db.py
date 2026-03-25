import os
import hashlib
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Optional

import pandas as pd
import chromadb
from chromadb.utils import embedding_functions
from langchain_community.vectorstores.chroma import Chroma
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from langchain_community.retrievers import BM25Retriever
from langchain_classic.retrievers.ensemble import EnsembleRetriever
from langchain_google_vertexai import VertexAIEmbeddings
from langchain_community.embeddings import SentenceTransformerEmbeddings
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Backend selector
# ---------------------------------------------------------------------------

class VectorDBBackend(Enum):
    CHROMA = "chroma"
    FAISS = "faiss"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_doc_id(report: str, page_name: str) -> str:
    """Returns a collision-resistant document ID from report + page name."""
    raw = f"{report}||{page_name}"
    return hashlib.md5(raw.encode("utf-8"), usedforsecurity=False).hexdigest()


def _load_dataframe(report_file: Path, include_hidden: bool = False) -> pd.DataFrame:
    """
    Loads a page report file into a DataFrame, applying standard filters.

    Args:
        report_file: Path to an Excel or CSV page report.
        include_hidden: If False (default), rows where 'Is Visible' is False
                        are dropped before building the vector store.

    Returns:
        Filtered DataFrame ready for embedding.
    """
    if report_file.suffix == ".xlsx":
        df = pd.read_excel(report_file)
    elif report_file.suffix == ".csv":
        df = pd.read_csv(str(report_file))
    else:
        raise ValueError(f"Unsupported file type: {report_file.suffix}")

    # Drop pages without a description — they can't be embedded
    df.dropna(subset=["Description"], inplace=True)
    df = df[df["Description"].str.strip() != ""]

    # Optionally exclude hidden pages
    if not include_hidden and "Is Visible" in df.columns:
        before = len(df)
        df = df[df["Is Visible"] != False]  # noqa: E712  (handles bool & string)
        dropped = before - len(df)
        if dropped:
            print(f"  Excluded {dropped} hidden page(s). Pass --include-hidden to keep them.")

    return df


def _build_documents(df: pd.DataFrame) -> tuple[list[Document], list[str]]:
    """Converts a DataFrame into LangChain Documents with collision-resistant IDs."""
    documents = []
    ids = []
    for _, row in df.iterrows():
        doc = Document(
            page_content=row["Description"],
            metadata={k: str(v) for k, v in row.items() if k != "Description"},
        )
        documents.append(doc)
        ids.append(_make_doc_id(str(row.get("Report", "")), str(row.get("Page Name", ""))))
    return documents, ids


def format_results(results: list[Document]) -> list[dict]:
    """
    Converts a list of result Documents into plain dicts for easy printing or
    downstream processing.

    Returns:
        List of dicts with keys: rank, page, report, is_visible, description.
    """
    formatted = []
    for i, doc in enumerate(results):
        formatted.append({
            "rank": i + 1,
            "page": doc.metadata.get("Page Name", "N/A"),
            "report": doc.metadata.get("Report", "N/A"),
            "is_visible": doc.metadata.get("Is Visible", "N/A"),
            "description": doc.page_content,
        })
    return formatted


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class BaseVectorDB(ABC):
    """Common interface for all vector DB backends."""

    @abstractmethod
    def create_pagedb_from_file(
        self,
        report_file: Path,
        collection_name: str = "pbi_pages",
        include_hidden: bool = False,
    ) -> None:
        """Build (or rebuild) the vector store from a page report file."""

    @abstractmethod
    def query_pages(
        self,
        query: str,
        collection_name: str = "pbi_pages",
        top_k: int = 5,
    ) -> list[Document]:
        """Query the vector store and return the top-k results."""


# ---------------------------------------------------------------------------
# ChromaDB backend
# ---------------------------------------------------------------------------

class ChromaVectorDB(BaseVectorDB):
    """Vector DB backend backed by ChromaDB with hybrid semantic + keyword search."""

    def __init__(self, db_path: Optional[Path] = None):
        load_dotenv()
        self.gcp_project = os.getenv("GCP_PROJECT")

        if db_path:
            self.client = chromadb.PersistentClient(path=str(db_path))
        else:
            self.client = chromadb.PersistentClient()

        if self.gcp_project:
            self.embedding_function = VertexAIEmbeddings(
                model_name="text-embedding-005", project=self.gcp_project
            )
            # Chroma-native function used only for collection creation
            self._chroma_ef = None
        else:
            self.embedding_function = SentenceTransformerEmbeddings(
                model_name="all-MiniLM-L6-v2"
            )
            self._chroma_ef = embedding_functions.SentenceTransformerEmbeddingFunction(
                model_name="all-MiniLM-L6-v2"
            )

    def create_pagedb_from_file(
        self,
        report_file: Path,
        collection_name: str = "pbi_pages",
        include_hidden: bool = False,
    ) -> None:
        """
        Creates or updates a ChromaDB collection from a page report file.

        Documents are upserted by a stable MD5 ID so running the command twice
        on the same file never creates duplicate entries.

        Args:
            report_file: Path to the Excel or CSV page report.
            collection_name: Chroma collection name.
            include_hidden: When False, hidden pages are excluded from the store.
        """
        print(f"\n--- Building ChromaDB from {report_file.name} ---")
        df = _load_dataframe(report_file, include_hidden=include_hidden)

        if df.empty:
            print("No pages with descriptions found. Vector DB not updated.")
            return

        print(f"Embedding {len(df)} pages into collection '{collection_name}'...")
        documents, ids = _build_documents(df)

        vector_store = Chroma(
            client=self.client,
            collection_name=collection_name,
            embedding_function=self.embedding_function,
        )

        # Upsert in batches of 250 (Vertex AI API limit)
        batch_size = 250
        for i in range(0, len(documents), batch_size):
            batch_docs = documents[i : i + batch_size]
            batch_ids = ids[i : i + batch_size]
            print(f"  Upserting batch {i // batch_size + 1} ({len(batch_docs)} docs)...")
            vector_store.add_documents(documents=batch_docs, ids=batch_ids)

        print(f"Done. {len(documents)} pages in '{collection_name}'.")

    def query_pages(
        self,
        query: str,
        collection_name: str = "pbi_pages",
        top_k: int = 5,
    ) -> list[Document]:
        """
        Queries the ChromaDB collection using hybrid search (semantic + BM25 keyword).

        Args:
            query: Natural-language search string.
            collection_name: Chroma collection to search.
            top_k: Number of results to return per retriever.

        Returns:
            Ranked list of Document objects.
        """
        print(f"\n--- Querying ChromaDB for: '{query}' ---")

        vector_store = Chroma(
            client=self.client,
            collection_name=collection_name,
            embedding_function=self.embedding_function,
        )
        embedding_retriever = vector_store.as_retriever(search_kwargs={"k": top_k})

        # Load all docs for BM25 keyword retriever
        collection = self.client.get_collection(name=collection_name)
        all_docs_data = collection.get(include=["metadatas", "documents"])
        all_docs = [
            Document(page_content=content, metadata=all_docs_data["metadatas"][i])
            for i, content in enumerate(all_docs_data["documents"])
        ]

        keyword_retriever = BM25Retriever.from_documents(all_docs)
        keyword_retriever.k = top_k

        ensemble = EnsembleRetriever(
            retrievers=[embedding_retriever, keyword_retriever],
            weights=[0.6, 0.4],
        )
        results = ensemble.invoke(query)
        print(f"Found {len(results)} result(s).")
        return results


# ---------------------------------------------------------------------------
# FAISS backend
# ---------------------------------------------------------------------------

class FaissVectorDB(BaseVectorDB):
    """
    Vector DB backend backed by FAISS (pure local, no server required).

    The index is saved to / loaded from  <db_path>/<collection_name>.faiss
    on disk so it survives restarts.
    """

    def __init__(self, db_path: Optional[Path] = None):
        load_dotenv()
        self.gcp_project = os.getenv("GCP_PROJECT")
        self.db_path = db_path or Path.cwd() / "vector_db"
        self.db_path.mkdir(parents=True, exist_ok=True)

        if self.gcp_project:
            self.embedding_function = VertexAIEmbeddings(
                model_name="text-embedding-005", project=self.gcp_project
            )
        else:
            self.embedding_function = SentenceTransformerEmbeddings(
                model_name="all-MiniLM-L6-v2"
            )

    def _index_path(self, collection_name: str) -> Path:
        return self.db_path / collection_name

    def create_pagedb_from_file(
        self,
        report_file: Path,
        collection_name: str = "pbi_pages",
        include_hidden: bool = False,
    ) -> None:
        """
        Builds (or rebuilds) a FAISS index from a page report file.

        The index is persisted to disk at <db_path>/<collection_name>/.
        Re-running this command rebuilds the index from scratch (FAISS does not
        support incremental upsert, so a full rebuild is the safe approach).

        Args:
            report_file: Path to the Excel or CSV page report.
            collection_name: Sub-directory name for the saved index.
            include_hidden: When False, hidden pages are excluded.
        """
        print(f"\n--- Building FAISS index from {report_file.name} ---")
        df = _load_dataframe(report_file, include_hidden=include_hidden)

        if df.empty:
            print("No pages with descriptions found. FAISS index not updated.")
            return

        print(f"Embedding {len(df)} pages into FAISS index '{collection_name}'...")
        documents, _ = _build_documents(df)

        vector_store = FAISS.from_documents(documents, self.embedding_function)
        save_path = self._index_path(collection_name)
        vector_store.save_local(str(save_path))
        print(f"Done. Index saved to {save_path}.")

    def query_pages(
        self,
        query: str,
        collection_name: str = "pbi_pages",
        top_k: int = 5,
    ) -> list[Document]:
        """
        Queries a persisted FAISS index using hybrid search (semantic + BM25).

        Args:
            query: Natural-language search string.
            collection_name: Sub-directory name of the saved index.
            top_k: Number of results to return per retriever.

        Returns:
            Ranked list of Document objects.
        """
        print(f"\n--- Querying FAISS for: '{query}' ---")
        index_path = self._index_path(collection_name)

        if not index_path.exists():
            raise FileNotFoundError(
                f"No FAISS index found at '{index_path}'. "
                "Run 'build-db' first to create one."
            )

        vector_store = FAISS.load_local(
            str(index_path),
            self.embedding_function,
            allow_dangerous_deserialization=True,
        )
        embedding_retriever = vector_store.as_retriever(search_kwargs={"k": top_k})

        # Load all docs for BM25 — iterate via the public index-to-docstore-id mapping
        all_docs = [
            vector_store.docstore.search(doc_id)
            for doc_id in vector_store.index_to_docstore_id.values()
        ]
        all_docs = [d for d in all_docs if isinstance(d, Document)]
        keyword_retriever = BM25Retriever.from_documents(all_docs)
        keyword_retriever.k = top_k

        ensemble = EnsembleRetriever(
            retrievers=[embedding_retriever, keyword_retriever],
            weights=[0.6, 0.4],
        )
        results = ensemble.invoke(query)
        print(f"Found {len(results)} result(s).")
        return results


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

class VectorDBFactory:
    """Creates the appropriate VectorDB backend instance."""

    @staticmethod
    def create(
        backend: VectorDBBackend = VectorDBBackend.CHROMA,
        db_path: Optional[Path] = None,
    ) -> BaseVectorDB:
        """
        Instantiates and returns a vector DB backend.

        Args:
            backend: Which backend to use (CHROMA or FAISS).
            db_path: Root directory for persisting the index/store.

        Returns:
            A BaseVectorDB instance.
        """
        if backend == VectorDBBackend.CHROMA:
            return ChromaVectorDB(db_path=db_path)
        elif backend == VectorDBBackend.FAISS:
            return FaissVectorDB(db_path=db_path)
        else:
            raise ValueError(f"Unknown backend: {backend}")


# ---------------------------------------------------------------------------
# Legacy alias — keeps any existing code that imported `VectorDB` working
# ---------------------------------------------------------------------------

VectorDB = ChromaVectorDB


# ---------------------------------------------------------------------------
# Quick manual test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    db_path = Path(__file__).parent.parent / "vector_db"
    db = VectorDBFactory.create(backend=VectorDBBackend.CHROMA, db_path=db_path)

    file_path = Path(__file__).parent.parent / "output" / "pages_2026-03-25_10-00-00_enhanced.xlsx"
    db.create_pagedb_from_file(report_file=file_path, include_hidden=False)

    print("\n--- QUERYING ---")
    results = db.query_pages(query="delayed items")
    for entry in format_results(results):
        print(f"\n[{entry['rank']}] {entry['report']} / {entry['page']}")
        print(f"    {entry['description']}")
