from diagrams import Cluster, Diagram, Edge, Group
from diagrams.custom import Custom
from diagrams.gcp.compute import Functions
from diagrams.gcp.storage import GCS
from diagrams.gcp.devtools import Scheduler
from diagrams.onprem.database import PostgreSQL


with Diagram("Project Architecture", show=False):

    # GCP INFRASTRUCTURE
    with Group("GCP Infrastructure"):
        with Cluster("Orchestration"):
            scheduler = Scheduler("Scheduler")
        with Cluster("Cloud Functions"):
            with Cluster("Crawlers"):
                crawlers = [
                    Functions("Fighter Scraper"),
                    Functions("Stats Scraper")
                ]
            processing = Functions("Data Processing")

        with Cluster("Storage"):
            storage = [
                GCS("GCS"),
                PostgreSQL("Postgres")
            ]

    # PROJECT INFRASTRUCTURE
    with Cluster("Analytical Web App"):
        with Cluster("Server"):
            backend = Custom("", "./custom-flask.png")

        with Cluster("Client"):
            frontend = Custom("", "./custom-dash.png")

    scheduler >> crawlers[0]
    crawlers >> storage[0]
    processing >> storage[0]
    storage[0] >> storage[1]
    storage[1] - Edge(style='dashed', color="red") - backend - Edge(style='dashed', color="red") - frontend
    
    

