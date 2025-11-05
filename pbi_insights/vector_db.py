import os

import pandas as pd
from pathlib import Path
import chromadb
from chromadb.utils import embedding_functions
from langchain_community.vectorstores.chroma import Chroma
from langchain_core.documents import Document
from langchain_community.retrievers import BM25Retriever
from langchain_classic.retrievers.ensemble import EnsembleRetriever
from langchain_google_vertexai import VertexAIEmbeddings
from dotenv import load_dotenv

class VectorDB:
    def __init__(self, db_path: Path = None):
        if db_path:
            self.client = chromadb.PersistentClient(path=str(db_path))
        else:
            self.client = chromadb.PersistentClient()

        load_dotenv()
        self.gcp_project = os.getenv("GCP_PROJECT")

        if self.gcp_project:
            self.embedding_function = VertexAIEmbeddings(model_name="text-embedding-005", project=self.gcp_project)
        else:
            self.embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")


    def create_pagedb_from_file(self, report_file: Path, collection_name: str = "pbi_pages"):
        """
        Creates or updates a Chroma vector database from a page report file.

        It uses the 'Description' column for embedding and stores other columns as metadata.

        Args:
            report_file: Path to the page report Excel or CSV file.
            db_path: Path to persist the Chroma database.
            collection_name: Name of the collection within the database.
        """
        print(f"\n--- Creating Vector DB from {report_file.name} ---")

        if report_file.suffix == ".xlsx":
            df = pd.read_excel(report_file)
        elif report_file.suffix == ".csv":
            df = pd.read_csv(str(report_file))
        else:
            raise ValueError(f"Unsupported file type: {report_file.suffix}")

        # Filter out rows where the description is missing, as they cannot be embedded.
        df.dropna(subset=['Description'], inplace=True)
        df = df[df['Description'].str.strip() != '']

        if df.empty:
            print("No pages with descriptions found in the file. Vector DB not created.")
            return

        print(f"Found {len(df)} pages with descriptions to add to the vector database.")

        documents = [
            Document(
                page_content=row['Description'],
                metadata={k: v for k, v in row.items() if k != 'Description'}
            )
            for _, row in df.iterrows()
        ]
        ids = [f"{doc.metadata['Report']}-{doc.metadata['Page Name']}" for doc in documents]
        
        # Initialize the Chroma vector store 
        vector_store = Chroma(
            client=self.client,
            collection_name=collection_name,
            embedding_function=self.embedding_function
        )

        # Batch process the documents to stay within the API limits (250 per call for Vertex AI)
        batch_size = 250
        for i in range(0, len(documents), batch_size):
            batch_docs = documents[i:i + batch_size]
            batch_ids = ids[i:i + batch_size]
            
            print(f"Processing batch {i//batch_size + 1}: adding {len(batch_docs)} documents...")
            
            # Use the add_documents method to add documents in batches
            vector_store.add_documents(documents=batch_docs, ids=batch_ids)

        print(f"\nSuccessfully added/updated a total of {len(documents)} pages in the '{collection_name}' collection.")

    def query_pages(self, query: str, collection_name: str = "pbi_pages", top_k: int = 5) -> list[Document]:
        """
        Queries the vector database using a hybrid search approach (semantic + keyword).

        Args:
            query: The search query string.
            collection_name: The name of the collection to query.
            top_k: The number of top results to return from each retriever.

        Returns:
            A list of relevant Document objects, ranked by the ensemble retriever.
        """
        print(f"\n--- Querying for: '{query}' ---")

        # Init embedding retriever (semantic search)
        vector_store = Chroma(
            client=self.client,
            collection_name=collection_name,
            embedding_function=self.embedding_function
        )
        embedding_retriever = vector_store.as_retriever(search_kwargs={"k": top_k})

        # Init keyword retriever (keyword search)
        collection = self.client.get_collection(name=collection_name)
        all_docs_data = collection.get(include=["metadatas", "documents"])

        all_docs = [
            Document(page_content=content, metadata=all_docs_data['metadatas'][i])
            for i, content in enumerate(all_docs_data['documents'])
        ]

        keyword_retriever = BM25Retriever.from_documents(all_docs)
        keyword_retriever.k = top_k

        # Combine retrievers and invoke it with query
        ensemble_retriever = EnsembleRetriever(
            retrievers=[embedding_retriever, keyword_retriever],
            weights=[0.6, 0.4]
        )

        results = ensemble_retriever.invoke(query)
        print(f"Found {len(results)} relevant results.")
        return results


if __name__ == "__main__":
    db_path = Path.cwd().parent / "vector_db"
    db = VectorDB(db_path=db_path)
    file_path = Path.cwd() / ".." / "output" / "pages_2025-11-04_12-41-39_enhanced.xlsx"
    db.create_pagedb_from_file(report_file=file_path)

    print("\n--- QUERYING DATABASE ---")
    search_query = "Searching for delayed items"
    query_results = db.query_pages(query=search_query)

    for i, doc in enumerate(query_results):
        print(f"\nResult {i + 1}:")
        print(f"  - Page: {doc.metadata.get('Page Name', 'N/A')}")
        print(f"  - Report: {doc.metadata.get('Report', 'N/A')}")
        print(f"  - Description: {doc.page_content}")
