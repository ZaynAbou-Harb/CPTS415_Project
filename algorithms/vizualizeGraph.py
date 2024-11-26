from neo4j import GraphDatabase
from neo4j.graph import Node, Relationship
import networkx as nx
import matplotlib.pyplot as plt
import math
import sys
import os


def get_edges_and_nodes(search_query, search_type, nNodes):
    """
    Executes a Cypher query based on the search type and query provided
    and returns the results as nodes and edges.

    :param search_query: The value to search for in the database.
    :param search_type: The type of search ('primaryName' or 'primaryTitle').
    :return: A dictionary containing nodes and edges.
    """
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from algorithms.credentialHandler import getCredentials
    url, username, password = getCredentials()
    driver = GraphDatabase.driver(url, auth=(username, password))

    # Define the Cypher query
    if search_type == 'primaryName':
        query = """
        MATCH (p:Person {primaryName: $search_query})-[r:WORKED_ON]->(m:Movie)<-[b:WORKED_ON]-(a:Person)
        RETURN p, r, m, b, a LIMIT $nNodes
        """
    elif search_type == 'primaryTitle':
        query = """
        MATCH (m:Movie {primaryTitle: $search_query})<-[r:WORKED_ON]-(p:Person)
        RETURN m, r, p LIMIT $nNodes
        """
    else:
        raise ValueError("Invalid search type")

    # Initialize the output
    nodes = {}
    edges = []

    # Execute the query
    with driver.session() as session:
        results = session.run(query, search_query=search_query, nNodes=nNodes)

        for record in results:
            for key, value in record.items():
                if isinstance(value, Node):  # Process nodes
                    node_id = value.id
                    if node_id not in nodes:
                        nodes[node_id] = {
                            "id": node_id,
                            "labels": list(value.labels),
                            "properties": dict(value),
                        }
                elif isinstance(value, Relationship):  # Process relationships
                    edges.append({
                        "startNode": value.start_node.id,
                        "endNode": value.end_node.id,
                        "type": value.type,
                        "properties": dict(value),
                    })

    driver.close()

    # Convert nodes dict to a list
    node_list = list(nodes.values())

    # Return the nodes and edges
    return {"nodes": node_list, "edges": edges}
        


def create_and_save_custom_graph(graph_data, search_query, search_type, nNodes, output_file):
    """
    Creates a customized graph layout with no overlapping nodes and saves it as a PNG file.

    :param graph_data: A dictionary containing 'nodes' and 'edges'.
    :param search_query: The search query used to filter the graph.
    :param search_type: The type of search ('primaryName' or 'primaryTitle').
    :param output_file: The file path to save the graph image.
    """
    G = nx.DiGraph()  # Directed graph

    # Add nodes with their attributes
    for node in graph_data['nodes']:
        G.add_node(node['id'], labels=node['labels'], **node['properties'])

    # Add edges with their attributes
    for edge in graph_data['edges']:
        G.add_edge(edge['startNode'], edge['endNode'], type=edge['type'], **edge['properties'])

    # Define colors for nodes based on their labels
    node_colors = []
    for node_id in G.nodes():
        labels = G.nodes[node_id].get('labels', [])
        if 'Movie' in labels:
            node_colors.append('red')  # Movie nodes are red
        elif 'Person' in labels:
            node_colors.append('blue')  # Person nodes are blue
        else:
            node_colors.append('gray')  # Default color for other nodes

    # Custom positioning based on search type
    pos = {}
    if search_type == "primaryName":
        # Center the searched person
        center_node = next(
            node['id'] for node in graph_data['nodes'] if node['properties'].get('primaryName') == search_query
        )
        pos[center_node] = (0, 0)

        # Arrange movies in a circular pattern around the person
        movies = [node for node in graph_data['nodes'] if 'Movie' in node['labels']]
        angle_step = 2 * math.pi / max(len(movies), 1)
        radius = max(len(movies), 1)  # Adjust radius dynamically
        for i, movie in enumerate(movies):
            pos[movie['id']] = (
                radius * math.cos(i * angle_step),
                radius * math.sin(i * angle_step),
            )

            # Arrange people in smaller circles around each movie
            related_people = [
                node
                for node in graph_data['nodes']
                if node['id'] in [edge['startNode'] for edge in graph_data['edges'] if edge['endNode'] == movie['id']]
            ]
            sub_angle_step = 2 * math.pi / max(len(related_people), 1)
            sub_radius = 3  # Smaller radius for people around each movie
            for j, person in enumerate(related_people):
                pos[person['id']] = (
                    pos[movie['id']][0] + sub_radius * math.cos(j * sub_angle_step),
                    pos[movie['id']][1] + sub_radius * math.sin(j * sub_angle_step),
                )
        pos[center_node] = (0, 0)

    elif search_type == "primaryTitle":
        # Center the searched movie
        center_node = next(
            node['id'] for node in graph_data['nodes'] if node['properties'].get('primaryTitle') == search_query
        )
        pos[center_node] = (0, 0)

        # Arrange people in a circular pattern around the movie
        people = [node for node in graph_data['nodes'] if 'Person' in node['labels']]
        angle_step = 2 * math.pi / max(len(people), 1)
        radius = max(len(people), 1)  # Adjust radius dynamically
        for i, person in enumerate(people):
            pos[person['id']] = (
                radius * math.cos(i * angle_step),
                radius * math.sin(i * angle_step),
            )

    # Plot the graph
    plt.figure(figsize=(24, 24))  # Larger figure size for better readability
    nx.draw(
        G,
        pos,
        with_labels=False,
        node_color=node_colors,
        edge_color='black',
        font_size=32,
        font_color='white',
    )

    # Add labels for nodes
    node_labels = {
        node_id: G.nodes[node_id].get('primaryName', '') or G.nodes[node_id].get('primaryTitle', '')
        for node_id in G.nodes()
    }
    nx.draw_networkx_labels(G, pos, labels=node_labels, font_size=8)

    # Save the graph to a file
    plt.title("Customized Graph Visualization", fontsize=16)
    plt.savefig(output_file, dpi=300)
    plt.close()  