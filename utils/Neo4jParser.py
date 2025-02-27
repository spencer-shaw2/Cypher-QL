from neo4j import EagerResult
from neo4j.graph import Node, Relationship, Path
import pandas as pd

class Neo4jParser:
    """
    This class contains functions to help with parsing results from Neo4j.
    """
    @staticmethod
    def process_node(node: Node) -> dict:
        """ 
        Parses the result from the query into a more consumable dictionary

        Args:
            A neo4j.graph.Node object 
        
        Returns:
            A dictionary with the following defintions for each key:
                * "elementId" -> A unique identifier of each object returned from the graph for any given transaction.
                IMPORTANT!! Not that each 'elementId' may not be the same across transactions. Meaning each time the 
                graph is queried, it's possible that a new 'elementId' is generated for the same object.
                * "labels" -> The label(s) of the node as it appears in the graph.
                * "properties" -> The properties of the node as represented in the graph.
        """
        if isinstance(node, Node):
            return {
                "elementId": node.element_id,
                "labels": node.labels,
                "properties": dict(node.items())
            }
        else:
            raise ValueError(f"Input value `node` is not of type neo4j.graph.Relationship. Type {type(node)} passed.")


    @staticmethod
    def process_relationship(relationship: Relationship) -> dict:
        """ 
        Parses the result from the query into a more consumable dictionary.

        Args:
            A neo4j.graph.Relationship object 
        
        Returns:
            A dictionary with the following defintions for each key:
                * "startNode" -> In a "source/target" diagram, the start node is the source of the relationship. The 
                relationship comes from the 'startNode'.
                    - "elementId" -> A unique identifier of each object returned from the graph for any given transaction.
                    IMPORTANT!! Not that each 'elementId' may not be the same across transactions. Meaning each time the 
                    graph is queried, it's possible that a new 'elementId' is generated for the same object.
                    - "labels" -> The label(s) of the node as it appears in the graph.
                    - "properties" -> The properties of the node as represented in the graph.
                * "elementId" -> The UID for the relationship. Please see "startNode" -> "elementId".
                * "type" -> The "label" of the relationship, or its name.
                * "properties" -> The properties of the relationship as represented in the graph.
                * "endNode" - > In a "source/target" diagram, the end node is the target of the relationship. The 
                relationship points towards the 'endNode'.
                    - "elementId" -> The UID for the "endNode". Please see "startNode" -> "elementId".
                    - "labels" -> The label(s) of the node as it appears in the graph.
                    - "properties" -> The properties of the node as represented in the graph.
        """
        if isinstance(relationship, Relationship):
            return {
                "startNode": {
                    "elementId": relationship.nodes[0].element_id,
                    "labels": relationship.nodes[0].labels,
                    "properties": dict(relationship.nodes[0].items())
                    },
                "elementId": relationship.element_id,
                'type': relationship.type,
                'properties': dict(relationship.items()),
                "endNode": {
                    "elementId": relationship.nodes[1].element_id,
                    "labels": relationship.nodes[1].labels,
                    "properties": dict(relationship.nodes[1].items())
                }
            }
        else:
            raise ValueError(f"Input value `relationship` is not of type neo4j.graph.Relationship. Type {type(relationship)} passed.")
        

    @staticmethod
    def process_path(path: Path) -> dict:
        """ 
        Parses the result from the query into a more consumable dictionary.

        Args:
            A neo4j.graph.Path object 
        
        Returns:
            A dictionary with the following defintions for each key:
                * "startNodeElementId" -> A unique identifier of each object returned from the graph for any given transaction.
                IMPORTANT!! Not that each 'elementId' may not be the same across transactions. Meaning each time the 
                graph is queried, it's possible that a new 'elementId' is generated for the same object.
                * "nodes" -> A list of processed nodes, please see doc string for `process_node` function. The length of the list
                will depend on the number of nodes specified in the path expression.
                * "relationships" -> A list of processed relationships, please see doc string for `process_relationship` function.
                The length of the list will depend on the number of relationships specific in the path expression.
                * "endNodeElementId" -> A UID for the "end node". Please see "startNodeElementId".
        """
        if isinstance(path, Path):
            return {
                "startNodeElementId": path.start_node.element_id,
                "nodes": [Neo4jParser.process_node(node) for node in path.nodes],
                "relationships": [Neo4jParser.process_relationship(relationship) for relationship in path.relationships],
                "endNodeElementId": path.end_node.element_id
            }
        else:
            raise ValueError(f"Input value `path` is not of type neo4j.graph.Path. Type {type(path)} passed.")
        
    
    @staticmethod
    def parse(result: EagerResult, verbose: bool = False, return_table: bool = True) -> dict | pd.DataFrame:
        """ 
        A helper function to optionally print out some helpful metadata around the query and return
        a dictionary similar to what you would see in Neo4j Browser 'Table' view.

        Args:
            result -> The EagerResult object returned by Neo4j
            verbose -> Optional parameter to print out informative query metadata or not
            return_table -> Optional parameter to return the data as a dataframe or table

        Returns:
            A list of records as dictionaries or table
        """
        start = result.summary.result_available_after
        finish = start + result.summary.result_consumed_after
        print(f"Started streaming {len(result.records)} records after {start} ms and completed after {finish} ms.\n")
        
        if verbose:
            print(f"Query executed against database: '{result.summary.database}': {result.summary.query}")

        # Parse the initial EagerResult
        data: list[list[list[tuple[str, Path | Relationship | Node]]]] = [record.items() for record in result.records]
        # Determine the column headers required based on the number of variables returned in the query.
        column_headers = list(set([variable[0] for record in data for variable in record]))
        # The final processed dictionary to be returned at the end
        processed_records: dict[str, list[dict]] = {key: [] for key in column_headers}

        # 'data' holds EagerResult parsed data represented as a list for each record returned by the query
        for i in range(len(data)):
            # 'var_returned' holds a single record of lists where each list is a data point for each variable returned
            for var_returned in data[i]:
                # 'data_object' iterates through the variable name (column value) at index 0, and beyond that and individual data point returned by the query
                for index, data_object in enumerate(var_returned):
                    # The first index will hold the variable name that will help us determine how to store this data tabularly
                    if index == 0:
                        column_header = str(data_object)
                        continue
                    
                    # After index 0, each 'data_object' will contain actual data returned from the graph
                    # if isinstance(data_object, list):
                    #     # Paths like to return neo4j.graph objects in lists, while non paths do not. So, we need to handle this
                    #     data_object = data_object[0]

                    if isinstance(data_object, Node):
                        processed_records[column_header].append(Neo4jParser.process_node(data_object))
                    elif isinstance(data_object, Relationship):
                        processed_records[column_header].append(Neo4jParser.process_relationship(data_object))
                    elif isinstance(data_object, Path):
                        processed_records[column_header].append(Neo4jParser.process_path(data_object))
                    else:
                        processed_records[column_header].append(data_object)
                        # raise ValueError(f"Unexpected datatype encountered. Of type: {type(data_object)}")
        
        # If the result is length of 1 and index 0 is a list, then return the inner list
        for key in processed_records.keys():
            if (len(processed_records[key]) == 1) & (isinstance(processed_records[key][0], list)):
                processed_records[key] = processed_records[key][0]

        if return_table:
            return pd.DataFrame(processed_records)
        else:
            return processed_records
        

    @staticmethod 
    def simple_parse(result: EagerResult, verbose: bool = False) -> dict:
        """ 
        A simpler way to parse results is to use the '.data()' method to return data from `EagerResult.results`.
        However, with simplicity does potentially compromise the amount of information gathered from each query.
        This is an alternative to `Neo4jParser.parse`.

        Args:
            result -> The EagerResult object returned by Neo4j
            verbose -> Optional parameter to print out informative query metadata or not

        Returns:
            A list of records as dictionaries
        """
        return [record.data() for record in result.records]