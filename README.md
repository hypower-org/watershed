Watershed
=========

A library for defining and connecting streams.  This library depends heavily on Manifold, which was developed by Zach Tellman ([Manifold](https://github.com/ztellman/manifold)).  

![alt tag](http://www.montgomerycountymd.gov/DEP/Resources/Images/water/Outreach/What_is_a_Watershed_rockingham-nc.gif)

The purpose of this library is to allow the definition and connection of streams of information in a graph-like structure called a Watershed.  A watershed is composed of - ideally - many rivers.  

A river is defined by a name, its tributaries, and a sieve.  The name is simply and identifier; tributaries are the source rivers that feed into the sink river; and the sieve defines how the information "flows" from the tributaries to the output.

For example: 

```clojure

(require '[manifold.streams :as s])

(river  :blue-nile [:beles] 

        (fn [x] (s/map identity (apply s/zip x))))

```

In particular, the first argument is the title; the second is a vector containing the tributaries of the rivers; and the third is the sieve.  This function will receive an argument of a list of Manifold streams which represent the tributaries. They are passed to the sieve in the order specified by the tributaries argument.  For a river, This function *must* return a Manifold stream which represents the output of the river.   
