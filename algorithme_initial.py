def duplicate(rdd):
  return rdd.union(rdd.map(lambda x : (x[1],x[0])))

def ccf_iterate(rdd):
  liste = []
  cpt = 0
  value_list = []
  r = duplicate(rdd).groupByKey().mapValues(list)
  for i in range(r.count()):
    value_list = []
    key = r.collect()[i][0]
    min_v = key
    for value in r.collect()[i][1]:
      if (value < min_v):
        min_v = value
      value_list.append(value)
    if(min_v < key):
      x = (key, min_v)
      if x not in liste:
        liste.append(x)
      for j in value_list:
        if(min_v != j):
          cpt = cpt+1
          y = (j,min_v)
          if y not in liste:
            liste.append(y)
  rdd_answer = spark.sparkContext.parallelize(liste)
  return rdd_answer,cpt

def resolution_algorithme(rdd):
  cpt=1
  while(cpt!=0):
    rdd,cpt=ccf_iterate(rdd)
  return rdd