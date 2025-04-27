# Automated Real Time Hospital Patient Data Analytics Platform on AWS

**📚 Project Overview**

This project builds a fully automated, real-time hospital patient data analytics system using AWS cloud services. It simulates hospital patient events, streams them into a cloud data lake, processes the data serverlessly, and visualizes actionable insights through dynamic dashboards — all without manual intervention.

**🏛️ Architecture Diagram**

![Architecture Diagram](https://github.com/vtooooooo/Automated-Real-Time-Hospital-Patient-Data-Analytics-Platform-on-AWS/blob/main/architecture/architecture.png?raw=true)

**🛠️ Tools and Technologies Used**

1. AWS Lambda – Simulating patient events and report generation
2. Amazon Kinesis Firehose – Real-time data streaming
3. Amazon S3 – Data lake for raw and processed reports
4. AWS Glue – Automated schema discovery and ETL
5. Amazon Athena – Serverless SQL querying
6. Amazon QuickSight – Building interactive dashboards
7. Amazon EventBridge – Scheduling and automation


**🛠️ How the System Works**

1. Simulate patient events (admissions, vitals, payments) using AWS Lambda.
2. Stream events through Kinesis Firehose into S3.
3. Glue Crawler updates schema every 5 minutes.
4. Athena queries allow real-time SQL querying on new data.
5. Lambda (scheduled daily) generates 17 analytical reports.
6. Reports stored in S3 are visualized via QuickSight dashboards.

**📊 Dashboard Overview**

Dashboard Name & Description

1. Patient Inflow Overview: Track daily admissions and reasons for visits
2. Hospital Load by Department: Monitor patient load and critical cases across departments
3. Financial Risk Monitoring: Analyze payment methods and uninsured patient counts
4. Doctor Critical Load: Track emergency doctors' load and emergency outcomes
5. Resource Utilization: Bed type usage and stay durations
6. Longest Stay Analysis: Identify patients with extended hospital stays



**🎯 Key Achievements**

1. Built a fully serverless, automated hospital data pipeline within AWS Free Tier.
2. Implemented real-time data streaming, automated ETL, and dynamic BI dashboards.
3. Designed scalable architecture handling continuous simulated patient events.
4. Enabled daily refresh and reporting without manual intervention.

**📚 Lessons Learned**

1. Optimizing Lambda to avoid timeout errors for large batch processing.
2. Managing IAM permissions securely between services.
3. Improving dashboard refresh strategies in QuickSight for real-time updates.

**🚀 Future Enhancements**

1. Connect QuickSight datasets directly to Athena for live dashboard refresh.
2. Implement real-time critical event alerting via SNS notifications.
3. Expand architecture to support multi-hospital data aggregation.

**📦 How to Deploy**

1. Clone the repository.
2. Deploy Lambda scripts.
3. Set up Kinesis Firehose, S3 buckets, Glue Crawlers, and Athena tables.
4. Create EventBridge rules for automation.
5. Import generated reports into QuickSight for visualization.





