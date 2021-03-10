def duplicate(edge):
  return edge.map(lambda x : (x[1],x[0])).union(edge)

def group_and_sort(edge):
  return edge.groupByKey().mapValues(list).mapValues(sorted)

def filter_good_tuple(edge):
  return edge.filter(lambda x: x[0]>x[1][0])

def CCF_iteration(element):
  liste=[]
  cpt=False
  cle=element[0]
  min_liste=element[1][0]
  del element[1][0]  
  for i in element[1] :
    liste.append((i,min_liste))
    cpt=True
  liste.append((cle,min_liste))
  return (liste,cpt)

def deroulement_CCF(edge):
  cpt=1
  while(cpt!=0):
    edge=duplicate(edge)
    edge=group_and_sort(edge)
    edge=filter_good_tuple(edge)
    edge=edge.map(lambda x: CCF_iteration(x))
    cpt=sum(edge.map(lambda x:x[1]).collect())
    edge=edge.flatMap(lambda x:x[0])
    print(edge.collect())
  return edge