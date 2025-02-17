# Apache Arrow examples
The purpose of this project is mainly to practice using Apache Arrow for cpp 
(https://arrow.apache.org/). For me personally, this is not a trivial task. 
Along the way, the idea arose to compare the performance of the different 
libraries, in particular Pandas, Polars and Pyarrow.

Some materials from the book “Pandas Workout 200 exercises that will make you 
a stronger data analyst" by Reuven M. Lerner (https://github.com/reuven/pandas-workout) were used for practical tasks. 

Mainly here you can find practical examples for Arrow in addition to those on 
the official website and repository https://github.com/apache/arrow/tree/main/cpp/examples

For simplicity, examples for C++ are located in header files, and for Python - in Jupyter Notebooks.
For cpp you need to select the corresponding part in main: Chapter_name_Part_Number (for example group_joining_part_2)

### To run cpp part:
```
mkdir build && cd build
cmake ..
make
./arrow
```
