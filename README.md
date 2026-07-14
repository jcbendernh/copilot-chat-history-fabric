# Copilot Studio Agents - Conversation Chat History using Microsoft Fabric

## Overview
For many Copilot Studio makers and administrators, pulling conversation transcript data from Dataverse and navigating Application Insights and Azure Log Analytics can be challenging. 

To address this, I created this repository, which allows you to easily view your Copilot Studio Agent conversation history in a Power BI report within a Fabric Workspace. 

Below are sample screenshots of the report.

![Conversation Summary](img/conversationsummary.png)
![Conversation Detail](img/conversationdetail.png)

### High Level Architecture and Process Flow
```mermaid
%%{init: {"themeVariables": {"fontSize": "12px"}}}%%
flowchart TD
	A[Create Fabric Workspace] 
	B[Connect Git repo to Workspace]
	C[Deploy workspace artifacts via Source Control in Git]
	D[Set up Dataverse Link to Fabric<br/>Dataverse shortcut tables<br/>available in Fabric]
	E[Run Initial Ingest Notebook<br/>Transform and load to<br/>curated Conversations table]
    F[Conversations Table<br/>Curated analytics-ready conversation history]
	G[Schedule Incremental Load Notebook<br/>Append only new <br/>conversation records]
	H[Semantic Model maps fields<br/>to report-friendly names]
    I[Copilot Chat History Report<br/>Summary and Detail views]


    A --> B --> C--> D --> E --> F --> H --> I
    F --> G
    G --> F

```

## To get started, please perform the following:
1. Create a new Fabric Workspace (for example, "Copilot Observability").
2. Clone this repository to your GitHub environment.
3. Add your cloned repository to your Fabric Workspace via Git integration under Workspace settings. For more information, see [Connect a workspace to a Git repo](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started?tabs=azure-devops%2CGitHub%2Ccommit-to-git#connect-a-workspace-to-a-git-repo).
4. In your Fabric Workspace, perform an Update under Source Control to pull the Lakehouse, Notebook, Semantic Model, and Power BI report into your workspace. For more information, see [Basic concepts in Git integration - Commits and updates](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process?tabs=Azure%2Cazure-devops#commits-and-updates).
5. Set up the Dataverse Link to Microsoft Fabric to create shortcuts to the following tables: **ConversationTranscript** (conversationtranscript) and **User** (systemuser) in your Fabric Workspace. When finished, a new lakehouse beginning with "dataverse" will appear in your workspace. For more information, see [Link to Microsoft Fabric](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/fabric-link-to-data-platform).  


## Fabric Workspace Components
This repository contains the following items in the fabric subfolder, which will be deployed to your workspace when you perform the Source Control Update:
- **CopilotObservability**: A Lakehouse with schema enabled.
- **Conversations - Initial Ingest**: This notebook performs an initial load of Dataverse conversationtranscript data, parsing the JSON content into individual conversation messages and enriching them with user details. It then writes the curated results to a copilotconversation table in the CopilotObservability lakehouse and records the current source table version as a baseline for future incremental processing.
- **Conversations - Incremental Load**: This notebook performs an incremental load of Dataverse conversationtranscript data using Delta Change Data Feed, parses and explodes the JSON content into individual conversation messages, enriches them with user details, and appends the results into a curated copilotconversation table. It then updates a baseline version checkpoint so that future runs only process new changes.
- **Copilot Chat History Semantic Model**: This semantic model transforms field names from the dbo.copilotconversation delta table into user-friendly names for reporting purposes.
- **Copilot Chat History Report**: This report uses the Copilot Chat History Semantic Model and contains two main pages:
    - **Conversation Summary Page**: A high-level dashboard showing overall conversation history for a specified time period. You can filter by individual Copilot Studio agent and communication channel. You can also drill through to any agent to see more details on the Conversation Detail page.
    - **Conversation Detail Page**: Displays individual conversations and shows the conversation history between users and the agent.
