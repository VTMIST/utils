# MIST utils
Utility Functions for data acq, parsing, things of that nature

Each dataset has it's own module. The most important function in each is import_subsystem(), which returns a tidy pandas dataframe.
There is a specific heirarchy to each dataset that is followd in the import process, but generally they are similar

Here are some specific notes:
## AALPIP
- Remote getters don't exist yet. This just grabs the local copy of your csv file, which you can only get from MIST members
- The grab operations aren't paralellized, and the csv's are broken up into many/day, so loading long time periods can be slow (400 ms /(system * day))
- The data *can* be loaded into an extended DataFrame that automagically labels the site by PG, but this feature will be modified in future versions for compatibility sake
-**Clean** import options only work for fluxgate data imports and break everthin else right now

## DTU, HALLEY, AGO
- These should work, even as a remote getter
- The datasets/instruments importable are not fully representative of what's available
- The use the old method of DataFrame generation, and are much slower and more memory intensive (no generators)

## THEMIS
- This was purpose built for a particular study, but it has the bones to sucessfully get files from the THEMIS ftp and get particular variables from them
- There's even crude caching

## Plotters, warehouse, etc.
- These probably are either very old or not useful to anyone outside of MIST, let alone without local access to our data.
