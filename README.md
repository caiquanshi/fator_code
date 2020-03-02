# Assignment2

## Style
In the original file, many variable names used acronym or a few letters which are not meaningful but confusing. Some lines formatted a string within a print statement and some calls an execute function and directly loop over the results. There are some commented out lines which has no functionality.

We replaced all the acronym with descriptive variable names using lowercase letters and dashes. For example, we replaced p_crs with paser_create_channel. For the lines that encapsulates a function within another one, we modified them by splitting the tasks, storing the results in a variable and passing that variable to the desired function call. All commented out code was deleted to avoid confusion.

## Structure
In twitch.py, create channel, parse top spam, get top spam, store chat log and query chat log are put under one if statements which violates the single responsibility principle. Also, this one giant function takes too many arguments which is likely to cause confusions to caller.

We factored out the if statements and made each a class in a file called classes.py to achieve Object Orientation. We utilized the DAO implementation and all five classes inherited this pattern. In this way, we are able to separate data manipulations from accessing and fetching data from the database. In addition, each 
In twitch.py, we created a function called argparse_init to separate argument parsing from function calling. We imported classes.py to twitch so that, in main, we could apply the methods defined in classes to accomplish a specific goal.
By doing so, it is a lot easier to spot a bug and fix it since we can test each function individually. 

## Error Handling
The original file does not contain any error handling and reports errors that should be handled by the program.

We used try-catch when querying the database or modifying the database and exceptions are handled by a logger with level ERROR, so that users will be notified when their operations cannot synchronize with the database.

## Logging
Twitch.py uses print statements as a means of debugging or acquiring the execution state of a function, which is the least ideal way to process information with different properties. Many unnecessary messages will be displayed to users which sometimes cause confusion and might disclose unwanted database information.

Logger is used to replace the print statements. Depending on different properties of the messages, they are logged with different level including DEBUG, INFO, and ERROR. Our logging level is set to ERROR and thus only ERROR level messages will be printed out to inform users of their invalid actions. By using a logger with level ERROR, unnecessary debugging information and execution information will not be shown to users. 

## Extensibility
The original twitch.py has little extensibility since everything is done in the if statements. We have to pile up elif statements when a new functionality needs to be added, which makes the code messy, unorganized and hard to debug. Also, it is nearly impossible to extend a function inside an elif statement block since there exists a strong cohesion within the block.

Since we have a separate module to store all five classes and their methods, we could add a new functionality simply by creating a new class inheriting DAO and adding methods to achieve the desired functionality. If we need to extend an existing functionality, we would add more methods to its corresponding class without changing the previous methods. Therefore, not much effort is required in terms of extending functionality and modifying functionality since there is little cohesion.  


## Object Orientation
Twitch.py is barely Object orientated since it did not use composed objects to complete complex tasks but rather did everything under the main function. 

We improved the object orientation by creating five objects where each represents a functionality and inherits the DAO superclass. Each object has its own methods and each method only does one job so that single responsibility principal is satisfied and cohesion is reduced to minimal. Each object gets initialized in twitch.py under the function main. Thus, main only calls methods related to the objects in a logical order to complete a complex task. 

