# 🫡**UFC API**🫡 

Welcome to my first attempt at a Data Engineering based project! I recently shifted my focus from Data Science to Data Engineering and figured I could draw inspiration from my most recent [ML project](https://github.com/RobertJG17/UFC-Predictor). I would like to develop an analytical web application with the data I am consolidating but, for now, my primary focus is developing a public facing API for those interested in the sport. 



Here is a high level overview of the project I provisioned with [Diagrams](https://diagrams.mingrammer.com/), a python library that utilizes a code based interface for creating architecture diagrams.

![Project Overview](https://github.com/RobertJG17/UFC-API/blob/main/architecture/project_architecture.png)



## **Asychronous Web Scraping** 🕷 
One hurdle I ran into early on into development was incredibly slow scripts. Due to each request executing sequentially I had to wait for one request to finish, parse the response, and load it into an array prior to sending out subsequent requests. Thankfully I found this [Coding Entrepeneurs](https://www.youtube.com/watch?v=6ow7xloFy5s) video that discussed the idea of *asynchronous* programming, which essentially allowed me to send concurrent requests and execute my parsing functions in parallel.

Below are some metrics I recorded that show the drastic difference in performance between my synchronous and asynchronous implementation.
* Synchronous Code ~ 5522 seconds | 90 mins | 1.5 hrs
* Asynchronous Code (asyncio + aiohttp) ~ 185.19 seconds | 3 mins | 0.05 hr
* Overall, the async execution time proved to be **96% faster**

## **TODO** ✅ 
- [X] Migrate Cloud Functions to local scripts and use Google Cloud SDK
- [X] Abstract repetitive code across scripts to a class for increased modularity and readability
- [X] Utilize environment variables to secure sensitive GCP parameters
- [ ] Enhance data quality checks
- [ ] Refine project file structure
- [ ] Increase crawler resilience / include better error handling for data anomolies
