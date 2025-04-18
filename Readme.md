# GTP_DE-Projects

This repository contains the projects and tasks completed as part of the Data Engineering Graduate Trainee Program at Amalitech. Each project assigned to me during the program will be stored in individual branches within this main repository, allowing for easy version control, tracking, and possible collaboration.

---
## Goals

- To gain hands-on experience with various data engineering concepts, tools, and technologies, including data pipelines, ETL processes, database management, and cloud computing.
- To demonstrate proficiency in building scalable, reliable, and efficient data infrastructure.
- To document key learnings, challenges, and solutions encountered throughout the program.

---
## Branching Structure

This repository follows a clean and organized branching model:

```
main
├── README.md (project overview)
├── prod_lab1
│   └── project files for lab 1 (merged from dev_lab1)
├── prod_lab2
│   └── project files for lab 2 (merged from dev_lab2)
├── prod_labN
│   └── project files for lab N (merged from dev_labN)
```

- **main**: Default branch with a clean overview. No development or project files live here.
- **prod_labX**: Production-ready branch for each lab project.
- **dev_labX**: Temporary development branch used while building each lab project.
  - Merged into its corresponding `prod_labX` branch upon completion.
  - Deleted after merge to maintain a clean history.

---

## Workflow

1. A new project is initiated in a `dev_labX` branch.
2. All development work (scripts, notebooks, documentation, etc.) occurs in this branch.
3. Upon final review and completion, the branch is merged into its corresponding `prod_labX` branch.
4. The `dev_labX` branch is deleted after successful merge.

This approach ensures:
- Clean separation of concerns between development and production-ready code.
- Easy rollback and change tracking.
- Consistent documentation and handoff between phases.
---

## Tools & Technologies

- **Programming Languages**: Python, SQL
- **Data Tools**: Apache Airflow, Apache Spark, DBT, Pandas
- **Databases**: PostgreSQL, MMSQL, and cloud databases
- **Cloud Platforms**: AWS

---

## Contribution Guidelines

Although this repository is primarily for individual learning, best practices such as meaningful commits, proper branch naming, and thorough documentation are followed to simulate real-world collaboration workflows.

---

## Next Steps

Upcoming labs will cover advanced data engineering concepts such as:
- Data orchestration with Airflow
- Stream processing with Kafka/Spark
- Data modeling with DBT
- Cloud pipeline deployment on AWS

Stay tuned for updates in each respective `prod_labX` branch!
