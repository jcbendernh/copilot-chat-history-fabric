# Copilot Conversation Chat History

**Under Construction**

## Overview
For many Copilot Studio makers and administrators it can be challenging to pull the conversation transcript data from Dataverse.  Thus, I created this repo that allows you to easily view the data in a Power BI report within a Fabric Workspace.  Below are some screenshots for reference.

![Conversation Summary](img/conversationsummary.png)
![Conversation Detail](img/conversationdetail.png)


## To get started, please perform the following:
1. Create a brand new Fabric Workspace.  I named mine Copilot Observability.
2. Clone this repo to your GitHub environment.
3. Add your newly cloned GitHub repo to your Fabric Workspace environment via the Git integration under Workspace settings. For more on this topic, check out [Connect a workspace to a Git repo - GitHub Connect](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started?tabs=azure-devops%2CGitHub%2Ccommit-to-git#connect-a-workspace-to-a-git-repo).
4. Within your Fabric Workspace, perform an Update under Source Control to pull the Lakehouse, Notebooks and Data Agent into your newly created workspace.  For more on this topic, check out [Basic concepts in Git integration - Commits and updates](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process?tabs=Azure%2Cazure-devops#commits-and-updates).

## Fabric Workspace Components
This repository contains the following items that will be deposited into your workspace once you perform the Update via Source Control referenced above.
- **F1** Lakehouse with schema enabled
- **Silver - All Tables**: This notebook takes the .csv files that are uploaded to the Lakehouse files from Kaggle and transforms them into delta tables in the Silver schema.  There are two variables to update in the notebook to point to the information for your environment.
    1. bronze_file_path - Set this to match your bronze file path for you will deposit the Kaggle CSV files.
    2. silver_schema - Set this to match your LAKEHOUSE.SCHEMA for your silver delta tables.
- **Gold - All Tables**: This notebook transforms all the tables into the Silver schema to delta tables that act as materialized views in the Gold schema.  There are two variables to update in the notebook to point to the information for your environment.
    1. silver_schema - Set this to match your LAKEHOUSE.SCHEMA for your silver delta tables.
    2. gold_schema - Set this to match your LAKEHOUSE.SCHEMA for your gold delta tables.
- **f1_agent**: This is a Fabric Data Agent that utilizes the tables in the f1.gold schema as it's data source.  It also includes comprehensive Data Source Description, Data Source Instructions, and Agent Instructions.   