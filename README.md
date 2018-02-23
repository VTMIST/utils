# utils
Utility Functions for data acq, parsing, things of that nature

Each dataset has it's own module. The most important function in each is import_subsystem(), which returns a tidy pandas dataframe.
There is a specific heirarchy to each dataset that is followd in the import process, but generally they are similar.