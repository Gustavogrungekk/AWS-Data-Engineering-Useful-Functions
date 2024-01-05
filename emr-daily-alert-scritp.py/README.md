## AWS EMR Workflow Monitor
Efficiently monitor your team's daily workflows on Amazon EMR with this script designed to simplify the process. The concept is straightforward: access cluster logs and steps to gather essential information, then effortlessly dispatch them using your organizational email on a daily basis. To ensure seamless execution, make sure you have an organizational email and a corresponding .csv with its credentials securely stored in your S3 bucket.
While CloudWatch Events and AWS SNS offer alternative approaches, this script was crafted from the ground up, providing a hands-on solution for those who prefer building everything from scratch. Feel free to integrate it into your workflow, and don't hesitate to contribute or share your insights to make it even better. Your feedback is appreciated!

#### Email output example
Pipeline Status EMR Data Report 01-01-2024
Good job team! 0 erros.

Summary:
Successful jobs: 152
Jobs that failed: 0
Total jobs: 152
Failed clusters: None

CREATION_DATE	CLUSTER_ID	CLUSTER_NAME	CLUSTER_STATE	CLUSTER_MESSAGE	TOTAL_JOBS	SUCCEEDED	FAILED
2024-01-01	CLUSTER_ID	EMR-DEV	WAITING	CLUSTER READY TO RUN STEPS.	0	0	0
2024-01-01	CLUSTER_ID	EMR-PROD-CLUSTER	TERMINATED	STEPS COMPLETED	1	1	0
2024-01-01	CLUSTER_ID	EMR-PROD-CLUSTER	TERMINATED	STEPS COMPLETED	37	37	0
2024-01-01	CLUSTER_ID	EMR-PROD-CLUSTER	TERMINATED	STEPS COMPLETED	7	7	0
2024-01-01	CLUSTER_ID	EMR-PROD-CLUSTER	TERMINATED	STEPS COMPLETED	54	54	0
2024-01-01	CLUSTER_ID	EMR-PROD-CLUSTER	TERMINATED	STEPS COMPLETED	50	50	0
2024-01-01	CLUSTER_ID	EMR-PROD-CLUSTER	TERMINATED	STEPS COMPLETED	2	2	0
2024-01-01	CLUSTER_ID	EMR-PROD-CLUSTER	TERMINATED	STEPS COMPLETED	1	1	0

![Alt text](email_output.PNG)