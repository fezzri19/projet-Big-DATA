from pyspark import SparkContext
sc = SparkContext("local", "App")

def ccf_iteration(edges):
  #for each edge (A, B) it generates the edge (B, A), so that A is in the adjacency list of B and vice versa
  rdd1 = edges.union(edges.map(lambda x : (x[1],x[0]))).groupByKey().mapValues(list)
  rdd2 = rdd1.collect()
  #a set () which will contain all the distinct pairs which will be generated at the end of this iteration
  liste_values = set()
  #counter which is initialized to 0
  cpt = 0
  for couple in rdd2 :
    #the minimum value of the adjacency list
    minimum = min(couple[1]) 
    if couple[0]>minimum:
      liste_values.add((couple[0],minimum))
      for value in couple[1]:
        if(value != minimum):
          cpt = cpt+1
          liste_values.add((value,minimum))
  edges_sortie = spark.sparkContext.parallelize(liste_values)
      
  return edges_sortie,cpt



def result_algo(rdd):
  cpt = 1
  nombre_iteration=0
  while(cpt!=0):
    rdd,cpt = ccf_iteration(rdd)
    nombre_iteration=nombre_iteration+1 
  return rdd,nombre_iteration


facebook = sc.textFile("dbfs:/FileStore/shared_uploads/riad.fezzoua@dauphine.eu/facebook_combined.txt")
rdd_exemple = facebook.map(lambda x : x.split(' ')).map(lambda x: (x[0],x[1]))

import time
temps = time.time()
res,compt = result_algo(rdd_exemple)
temps2 = time.time()
x= temps2-temps
print(x)
