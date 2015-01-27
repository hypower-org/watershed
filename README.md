Watershed
=========

A library for defining and connecting streams.

![alt tag](http://www.montgomerycountymd.gov/DEP/Resources/Images/water/Outreach/What_is_a_Watershed_rockingham-nc.gif) 

The purpose of this library is to allow the definition and connection of streams of information in a graph-like structure called a Watershed.  A watershed is composed of-ideally-many waterways.  

A waterway is defined by a name, its tributaries, and a sieve.  The name is the waterway's identifier; tributaries are the source waterways that feed into the sink waterway; and the sieve defines how the information "flows" from the tributaries to the output.

To use this library, include the following in your project file:

    [hypower-org/watershed "0.1.4"]

This work was funded by the National Science Foundation through grant CNS-1239221.




