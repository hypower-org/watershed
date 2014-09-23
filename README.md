Watershed
=========

A library for defining and connecting streams that depends heavily on Manifold, which was developed by Zach Tellman ([Manifold](https://github.com/ztellman/manifold)).  

![alt tag](http://www.montgomerycountymd.gov/DEP/Resources/Images/water/Outreach/What_is_a_Watershed_rockingham-nc.gif) 

The purpose of this library is to allow the definition and connection of streams of information in a graph-like structure called a Watershed.  A watershed is composed of-ideally-many waterways.  

A waterway is defined by a name, its tributaries, and a sieve.  The name is simply an identifier; tributaries are the source waterways that feed into the sink waterway; and the sieve defines how the information "flows" from the tributaries to the output.

For example: 

```clojure

(require '[manifold.streams :as s])

{:blue-nile 

  {:tributaries [:beles] 

   :sieve (fn [x] (s/map identity (apply s/zip x)))
             
   :type :river}}

```

In particular, the first field is the title; the second is a vector containing the tributaries of the rivers; and the third is the sieve.  This function will receive an argument of a list of Manifold streams that represent the tributaries. They are passed to the sieve in the order specified by the tributaries argument.  Additionally, there is a specified type for each waterway.  For a river, this function *must* return a Manifold stream that represents the output of the river.    

For convenience, there are two other types of waterways: *sources* and *estuaries*.  

Sources represent the beginning of a watershed, so they have no tributaries.  However, they do still have an output. 

For example:

```clojure

{:beles

 {:tributaries [] 
 
  :sieve (fn [] (s/periodically 1000 (fn [] [:water])))
  
  :type :source}}

```

Estuaries are the end of a watershed.  Thus, they have tributaries, but they do not have outputs.  Instead, they can return a value from their sieve which will be yielded when *ebb* is called on the watershed.

For example: 

```clojure

{:waterfall 

  {:tributaries [:blue-nile]
          
   :sieve (fn [x] (s/reduce conj (s/map identity x)))
   
   :type :estuary}}

```

Each of these allow for easy composition of systems: 

```clojure

(require '[manifold.streams :as s])

(def test-watershed 

    (-> 

       {:beles

         {:tributaries [] 
 
          :sieve (fn [] (s/periodically 1000 (fn [] [:water])))
  
          :type :source}
          
       :blue-nile 

          {:tributaries [:beles] 

          :sieve (fn [x] (s/map identity (apply s/zip x)))
             
          :type :river}
                 
        :waterfall 

          {:tributaries [:blue-nile]
          
           :sieve (fn [x] (s/reduce conj (s/map identity x)))
   
           :type :estuary}}
        
        compile*))

```
Once a watershed has been created, it can be started with 'compile*', which connects all of the rivers in the watershed via the supplied tributaries.  This name is tentative.

After a watershed has been started, 'ebb' can be used to close all of the rivers.  Calling ebb will return a map of estuaries to resulting values.  

```clojure

(ebb w)

=> {:waterfall << ... >>}

```




