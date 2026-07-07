# Copilot Conversation Chat History

**Under Construction**

## Overview
For many Copilot Studio makers and administrators it can be challenging to pull the conversation transcript data from Dataverse and understanding Application Insights and Log Analytics in Azure may be "a bridge too far".  

Thus, I created this repo that allows you to easily view the Copilot Conversation History in a Power BI report within a Fabric Workspace.  Below are some screenshots for reference.

![Conversation Summary](img/conversationsummary.png)
![Conversation Detail](img/conversationdetail.png)


## To get started, please perform the following:
1. Create a brand new Fabric Workspace.  I named mine Copilot Observability.
2. Clone this repo to your GitHub environment.
3. Add your newly cloned GitHub repo to your Fabric Workspace environment via the Git integration under Workspace settings. For more on this topic, check out [Connect a workspace to a Git repo - GitHub Connect](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started?tabs=azure-devops%2CGitHub%2Ccommit-to-git#connect-a-workspace-to-a-git-repo).
4. Within your Fabric Workspace, perform an Update under Source Control to pull the Lakehouse, Notebook, Semantic Model and Power BI report into your newly created workspace.  For more on this topic, check out [Basic concepts in Git integration - Commits and updates](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process?tabs=Azure%2Cazure-devops#commits-and-updates).
5. Setup the Dataverse Link to Microsoft Fabric to shortcut the following tables: **ConversationTranscript** (conversationtranscript) and **User** (systemsuer) into the Fabric Workspace that you created in step 1 above.  When finished, you should have a new lakehouse in your workspace that has a name that begins with dataverse.  For more on this topic check out the following article [Link to Microsoft Fabric](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/fabric-link-to-data-platform).  


## Fabric Workspace Components
This repository contains the following items that will be deposited into your workspace once you perform the Update via Source Control referenced above.
- **CopilotObservability**: Lakehouse with schema enabled.
- **Conversations**: This notebook takes the data contained in the conversationtranscript and system user tables of the dataverse shortcutted lakehouse and transforms and simplifies the complex nature of the columns needed and into the dbo.copilotconversation delta table in the CopilotObservability lakehouse.  There is one variable to update in the notebook to point to the information to your shortcutted Dataverse lakehouse.
- **Copilot Chat History Semantic Model**: This converts the fields names, listed in the  dbo.copilotsonversation delta table in the CopilotObservability lakehouse ,and makes them user friendly for reporting purposes.
- **Copilot Chat History Report**: This utilizes the Copilot Chat History Semantic Model and has 2 major components.
    - **Conversation Summary Page**: This is a high level dashboard that shows the shows the overall conversation history for a specific time period.  You can see if by Copilot Studio Agent and also the channel it is published to.  You can also drill through into any Copilot Studio Agent to see ore detail on the COnversation Detail Page.
    - **Conversation Detail Page**: - This provides you the ability to view each conversation and see the individual conversation history between the user and the agent.