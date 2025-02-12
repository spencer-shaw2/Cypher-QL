# Start Here

These notebooks will focus on using Cypher and interacting with a Knowledge Graph from a python environment using Neo4j's Python driver. The downside to this, is it's hard to visualize some of the queries being ran. It's highly encourage to switch between the notebook and your local Neo4j Desktop database.

To download Neo4j Desktop, follow the instructions [here.](https://neo4j.com/download/)


🛑 **IMPORTANT**

In the "./utils" directory is a helper file `Neo4jParser.py`. I've gone ahead and wrote some code in assisting with parsing the results of queries returned from Neo4j. According to Neo4j's [documentation](https://neo4j.com/docs/api/python-driver/current/api.html#), there are several methods to parse an `EagerResult`. Please go to the utils directory and open `Neo4jParser.py` to understand the functions available.
> If you are newer to Python, or only looking for the data behind your queries, use: `Neo4jParser.simple_parse()`. Please be aware that the `neo4j.Record.data()` method will look different from the results you see in Neo4j Browser. For this reason, it's encouraged to use `Neo4jParser.parse()`.

> For an all encompassing view of the data and to match a format more similar to what you will see from the Neo4j Browser, use: `Neo4jParser.parse()`. This is the recommended approach and the one that will be used throughout this series.


⚠️ **NOTICE**

**If working from WSL...**

When working from WSL2 and Neo4j Desktop is installed on the Windows side, you have to set up port forwarding. To do this, open a Powershell administrator window and run the following:
1. Start the default Neo4j Desktop database "Movies DBMS".
2. Run `ipconfig` to fetch your machine's IP address
    * From this point forward assume you have a Windows ip address of: '123.456.78.900'
3. Run `netsh interface portproxy set v4tov4 listenport=7687 listenaddress=123.456.78.900 connectport=7687 connectaddress=127.0.0.1`
    * NOTE: The 'listenport' and 'connectport' should be the same port your Neo4j database is running on.
4. To verify, run `netsh interface portproxy show v4tov4`
5. To disable the port forwarding, run `netsh interface portproxy delete v4tov4 listenport=1234 listenaddress=123.456.78.900`

If working from a windows/mac environment where Neo4j Desktop is installed, the default 'localhost' URI should be sufficient.
