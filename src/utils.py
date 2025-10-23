from typing import Any, Dict, List, Set
import re

def _recursive_find_fields(data_object: Any) -> Set[str]:
    """Recursively traverses a JSON-like object to find all referenced fields.

    This function is the main entry point for field discovery within a visual's
    data structure. It delegates to specialized helper functions based on the
    structure of the data it encounters.

    Args:
        data_object: The dictionary or list to search within.

    Returns:
        A set of all unique field names found.
    """
    found_fields = set()

    if isinstance(data_object, dict):
        # This is mostly for singleVisuals within a visual
        if "projections" in data_object:
            #if "columnProperties" in data_object:
            #    cp_alias_map = _get_cp_alias(data_object["columnProperties"])
            return _projections_fields(data_object["projections"])

        # This is mostly used for filters within a visual
        elif "expression" in data_object:
            return _recursive_find_fields(data_object["expression"])

        # Following two statements are for dataTransform objects
        elif ("queryMetadata" in data_object and
              data_object["queryMetadata"] is not None and
              "Select" in data_object["queryMetadata"]):
            return _queryMetadata_fields(data_object["queryMetadata"])

        elif("queryMetadata" in data_object and
              data_object["queryMetadata"] is None and
              "selects" in data_object):
            return _selects_fields(data_object["selects"])

        # This is the primary pattern for identifying a field or measure
        if "Property" in data_object and "Expression" in data_object:
            prop = data_object["Property"]
            expr = data_object.get("Expression", {})
            source_ref = expr.get("SourceRef", {})
            entity = source_ref.get("Entity")

            if entity and prop:
                found_fields.add(f"{entity}.{prop}")

        for value in data_object.values():
            found_fields.update(_recursive_find_fields(value))

    elif isinstance(data_object, list):
        for item in data_object:
            found_fields.update(_recursive_find_fields(item))

    return found_fields

def _projections_fields(data_object: Dict) -> Set[str]:
    """Extracts field names from a 'projections' object within a visual.

    This function iterates through the projections (like 'Values', 'Series', etc.),
    extracts the 'queryRef' for each field, and cleans it of any DAX wrapper
    functions (e.g., 'Sum(...)') to get the base field name. This structure is
    most commonly found in 'singleVisual' objects.

    Args:
        data_object: The dictionary from a 'projections' key.

    Returns:
        A set of base field names found in the projections.
    """
    found_fields = set()

    for description, projection in data_object.items():
        for value in projection:
            # Clean up PBI internal functions
            found_fields.update(_strip_dax_functions(value["queryRef"]))
            '''
            for query in all_queries:

                if query in alias_map:
                    entity = query.split(".")[0]
                    found_fields.add(f"{entity}.{alias_map[query]}")
                else:
                found_fields.add(query)'''

    return found_fields

def _get_cp_alias(column_properties: Dict) -> Dict[str, str]:
    """
    Creates an alias map from a visual's 'columnProperties' object.

    This is used to resolve display names back to their original field names,
    especially when metric units are included in the display name (e.g., "Sales [USD]").

    Args:
        column_properties: The dictionary from a 'columnProperties' key.

    Returns:
        A dictionary mapping the original field name to its cleaned display name.
    """
    cp_alias_map = {}

    # This pattern filters out metric units that could appear in the back of measure descriptions like "Sales [USD]"
    regex_pattern = r"(.*?)(?=\[[a-zA-ZΑ-Ωα-ω0-9_ &]*\])"

    for key, value in column_properties.items():
        try:
            measure = re.findall(regex_pattern, value.displayName)[0]
        except:
            measure = value
        cp_alias_map[key] = measure

    return cp_alias_map

def _strip_dax_functions(query: str) -> Set[str]:
    """
    Strips away DAX wrapper functions to extract the base field(s).

    This function recursively handles simple (e.g., Sum) and compound
    (e.g., Divide) DAX functions to find the underlying 'Table.Column' references.

    Args:
        query: The DAX query string, e.g., "Divide(Sum(Sales.Revenue), Count(Orders.ID))".

    Returns:
        A set of unique base fields found in the query.
    """

    raw_fields = set()
    wrapper_functions = {"Avg", "Count", "CountNonNull", "Max", "Median", "Min", "StandardDeviation", "Sum"}
    rec_wrapper_functions = {"Divide", "ScopedEval"}

    wrapper_pattern = r"\b(\w+)\("
    found_wrapper = re.findall(wrapper_pattern, query)

    if found_wrapper:
        # Handle stacked wrapper functions like Divide(Sum(Sales.SalesAmount), Count(Sales.SalesAmount))
        if found_wrapper[0] in rec_wrapper_functions:
            for sub_query in query[len(found_wrapper[0]):-1].split(","):
                raw_fields.update(_strip_dax_functions(sub_query))
            return raw_fields

        # Handle simple wrapper functions like Min(Sales.SalesAmount)
        elif found_wrapper[0] in wrapper_functions:
            # Find content between the first '(' and last ')'
            try:
                start_index = query.index('(') + 1
                end_index = query.rindex(')')
                return {query[start_index:end_index]}
            except ValueError:
                return {query}

        else:
            return {query}

    else:
        return {query}


def _queryMetadata_fields(queryMetadata: Dict) -> Set[str]:
    """Extracts field names from the 'Select' list within a 'queryMetadata' object.

    This structure is commonly found in 'dataTransform' objects.

    Args:
        queryMetadata: The dictionary object from a 'queryMetadata' key.

    Returns:
        A set of field names found in the select statements.
    """
    found_fields = set()
    for select_statement in queryMetadata["Select"]:
        found_fields.add(select_statement.get("Name",""))

    return found_fields

def _selects_fields(selects: List[Dict]) -> Set[str]:
    """Extracts field names from a 'selects' list.

    This structure is often a fallback within 'dataTransform' objects when
    'queryMetadata' is null.

    Args:
        selects: A list of dictionary objects from a 'selects' key.

    Returns:
        A set of field names found in the select statements.
    """
    found_fields = set()
    for select_statement in selects:
        found_fields.add(select_statement.get("queryName"))
    
    return found_fields
