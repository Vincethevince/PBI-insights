import os
from http.client import responses

from langchain_google_vertexai import ChatVertexAI
import json
import asyncio
from typing import List, Dict

class VertexModel:
    """
    A client for interacting with Google's Vertex AI Gemini models to generate documentation for DAX measures.

    This class encapsulates the logic for initializing the model, formatting prompts,
    processing measures in batches, and handling asynchronous API calls.
    """
    def __init__(self, model_name: str="gemini-2.5-flash", temperature: float=0.1, max_output_tokens: int=8192):
        """
        Initializes the VertexModel client.

        Args:
            model_name: The name of the Gemini model to use (e.g., "gemini-1.5-flash").
            temperature: The model's temperature setting, controlling randomness. Lower is more deterministic.
            max_output_tokens: The maximum number of tokens to generate in the response.
        """
        self.gcp_project = os.getenv("GCP_PROJECT")

        self.llm = ChatVertexAI(
            model_name=model_name,
            temperature=temperature,
            max_output_tokens=max_output_tokens,
            project=self.gcp_project
        )

    async def process_batch(self, measure_batch: List) -> dict:
        """
        Processes a single batch of measures to generate natural language descriptions.

        This method constructs a prompt with the provided measures, sends it to the
        Vertex AI model, and parses the JSON response.

        Args:
            measure_batch: A list of measures, where each object
                           is expected to have 'name' and 'expression' keys.

        Returns:
            A dictionary mapping measure names to their generated natural language descriptions.
        """
        measures_json = json.dumps(
            {m['name']: m['expression'] for m in measure_batch},
            indent=2
        )
        prompt = f"""
        For each DAX measure below, return a concise natural language description of the measure.
        Output **only valid JSON** where:
        - Keys = measure names
        - Values = concise natural language descriptions
        
        Examples: 
        "Total Sales": "Sums all values in the Sales column",
        "YoY Growth": Calculates year-over-year percentage change"
        
        **Input Measures (valid JSON):**
        ```json        
        {measures_json}
        """

        response = await self.llm.ainvoke(prompt)
        try:
            cleaned_content = response.content.strip()

            if cleaned_content.startswith("```json"):
                cleaned_content = cleaned_content[len("```json"):]
            elif cleaned_content.startswith("```"):
                cleaned_content = cleaned_content[len("```"):]

            if cleaned_content.endswith("```"):
                cleaned_content = cleaned_content[:-len("```")]

            cleaned_content = cleaned_content.strip()

            return json.loads(cleaned_content)

        except json.JSONDecodeError:
            print(f"Failed to parse batch: {response.content}")
            return {}

    async def process_all_measures(self, measures: List, batch_size: int=20) -> dict:
        """
        Processes a list of all measures by splitting them into batches and running them concurrently.

        Args:
            measures: A list of all measure dictionaries to be processed.
            batch_size: The number of measures to include in each concurrent batch.

        Returns:
            A dictionary containing the merged results from all processed batches,
            mapping measure names to their generated descriptions.
        """
        batches = [measures[i:i+batch_size] for i in range(0,len(measures),batch_size)]
        results = await asyncio.gather(*[self.process_batch(batch) for batch in batches])
        
        merged_results = {}
        for result in results:
            merged_results.update(result)
        
        return merged_results
        
if __name__ == "__main__":
    model = VertexModel()
    measures_example = [
        {"name": "Sales[Total Sales]", "expression": "SUM(Sales.Revenue)"},
        {"name": "Sales[Sales per month]", "expression": "DIVIDE(Sales[Total Sales], 12)"}
    ]

    response = asyncio.run(model.process_all_measures(measures=measures_example))
    for key, val in response.items():
        print(f"{key}: {val}")
