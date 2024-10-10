# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "19b7b582-c117-458b-b16f-47ee9bdefe4c",
# META       "default_lakehouse_name": "Lakehouse_SILVER",
# META       "default_lakehouse_workspace_id": "5b0eaee0-12a4-4652-93b8-0f08395fb823"
# META     }
# META   }
# META }

# CELL ********************

!pip install graphviz

# CELL ********************

DAG = {
    "activities": [
        {
            "name": "load_silver_customer_feedback", # activity name, must be unique
            "path": "load_silver_customer_feedback", # notebook path
            "timeoutPerCellInSeconds": 90, # max timeout for each cell, default to 90 seconds
            # "args": {"p1": "changed value", "p2": 100}, # notebook parameters
        },
        {
            "name": "load_silver_customers",
            "path": "load_silver_customers",
            "timeoutPerCellInSeconds": 120,
            # "args": {"p1": "changed value 2", "p2": 200}
        },
        {
            "name": "load_gold_happy_first_names",
            "path": "load_gold_happy_first_names",
            "timeoutPerCellInSeconds": 120,
            # "args": {"p1": "changed value 3", "p2": 300},
            # "retry": 1,
            # "retryIntervalInSeconds": 10,
            "dependencies": ["load_silver_customer_feedback", "load_silver_customers"] # list of activity names that this activity depends on
        }
    ]
}


# CELL ********************

from graphviz import Digraph

def generate_notebook_dependency_graph(json_data, show_legend=False, apply_color=False):
    """
    Sandeep Pawar  |  fabric.guru  | 1/30/24
    Visualize runMultiple DAG using GraphViz. 
    """
    dot = Digraph(comment='Notebook Dependency Graph')
    dot.attr(rankdir='TB', size='10,10')
    dot.attr('node', shape='box', style='rounded', fontsize='12')

    if show_legend:
        with dot.subgraph(name='cluster_legend') as legend:
            legend.attr(label='Legend', fontsize='12', fontcolor='black', style='dashed')
            legend.node('L1', 'No Dependencies', style='filled', fillcolor='lightgreen', rank='max')
            legend.node('L2', 'One Dependency', style='filled', fillcolor='lightblue', rank='max')
            legend.node('L3', 'Multiple Dependencies', style='filled', fillcolor='lightyellow', rank='max')
            legend.node('L4', 'Args Present (in label)', rank='max')



    dot.attr('node', rank='', color='black')
    for notebook in json_data["activities"]:
        notebook_name = notebook['name']
        args = notebook.get('args', {})
        label = f"{notebook_name}"
        if args:
            label += f"\nArgs: {', '.join([f'{k}={v}' for k, v in args.items()])}"

        if apply_color:
            num_dependencies = len(notebook.get('dependencies', []))
            color = 'lightgreen' if num_dependencies == 0 else 'lightblue' if num_dependencies == 1 else 'lightyellow'
        else:
            color = 'white'

        dot.node(notebook_name, label, style='filled', fillcolor=color)

        for dependency in notebook.get('dependencies', []):
            dot.edge(dependency, notebook_name)


    display(dot)

generate_notebook_dependency_graph(DAG, show_legend=True, apply_color=True)


# CELL ********************

mssparkutils.notebook.runMultiple(DAG)
