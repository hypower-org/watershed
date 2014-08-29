Watershed
=========

A library for defining and connecting streams that depends heavily on Manifold, which was developed by Zach Tellman ([Manifold](https://github.com/ztellman/manifold)).  

![alt tag](http://www.montgomerycountymd.gov/DEP/Resources/Images/water/Outreach/What_is_a_Watershed_rockingham-nc.gif) 

The purpose of this library is to allow the definition and connection of streams of information in a graph-like structure called a Watershed.  A watershed is composed of-ideally-many rivers.  

A river is defined by a name, its tributaries, and a sieve.  The name is simply an identifier; tributaries are the source rivers that feed into the sink river; and the sieve defines how the information "flows" from the tributaries to the output.

For example: 

```clojure

(require '[manifold.streams :as s])

(river watershed :blue-nile 

                 [:beles] 

                 (fn [x] (s/map identity (apply s/zip x))))

```

In particular, the first argument is the title; the second is a vector containing the tributaries of the rivers; and the third is the sieve.  This function will receive an argument of a list of Manifold streams that represent the tributaries. They are passed to the sieve in the order specified by the tributaries argument.  For a river, this function *must* return a Manifold stream that represents the output of the river.    

For convenience, there are two other types of rivers: *sources* and *estuaries*.  

Sources represent the beginning of a river, so they have no tributaries.  However, they do still have an output. 

For example:

```clojure

(source watershed :beles

                  (fn [] (s/periodically 1000 (fn [] [:water]))))

```


Estuaries are the end of a river.  Thus, they have tributaries, but they do not have outputs.  Instead, they have an optional Manifold deferred which can contain a result.  

For example: 

```clojure

(estuary  watershed :waterfall 

                    [:blue-nile]
          
                    (fn [x] (s/reduce conj x)))

```

Each of these allow for easy composition of systems: 

```clojure

(require '[manifold.streams :as s])

(def w (-> (watershed)

           (source :beles

                 (fn [] (s/periodically 1000 (fn [] [:water]))))
        
           (river :blue-nile 

                 [:beles] 

                 (fn [x] (s/map identity (apply s/zip x))))
                 
           (estuary :waterfall 

                 [:blue-nile]
          
                 (fn [x] (s/reduce conj x)))
        
           flow))

```
Once a watershed has been created, it can be started with 'flow'.  Flow connects all of the rivers in the watershed via the supplied tributaries.  

After a watershed has been started, 'ebb' can be used to close all of the rivers.  Calling ebb will return a map of estuaries to resulting deferreds.  

```clojure

(ebb w)

=> {:waterfall << ... >>}

```




