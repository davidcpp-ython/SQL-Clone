# SQL Clone in C

## Overview 📖
This project implements a simple SQLite-like database using C. It allows users to insert and retrieve data via a command-line interface, storing information in a file.

## Features ✨
- Insert new rows into the database
- Select and display stored rows
- Persistent storage using a file
- Simple command-line interaction 

## Installation ⚙️
To compile the program, use:
```sh
gcc main.c -o main
```

## Usage 🚀
Run the program with a database file:
```sh
./main database.db
```

## Commands 🖥️
- **Insert Data:**  
  ```sh
  insert <id> <username> <email>
  ```
- **Retrieve Data:**  
  ```sh
  select
  ```
- **Exit:**  
  ```sh
  .exit
  ```
  
## 🧪 Running Tests  

To validate the functionality of the database system, you can use the `tests.py` file. This script automates the testing of various database commands.  

## ✅ How to Run Tests  
1. Ensure you have Python installed.
2. Open a terminal in the project directory.  
3. Run the following command:  
   ```sh
   python3 tests.py

## Implementation Details 🔧
The database uses:
- **Paging** for memory efficiency
- **Serialization** and **deserialization** for data storage
- **Command parsing** for user input


