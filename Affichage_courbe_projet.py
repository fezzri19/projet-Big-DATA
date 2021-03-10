import numpy as np
import matplotlib.pyplot as plt
liste_rdd = [rdd_exemple1,rdd_exemple2,rdd_exemple3,rdd_exemple4,rdd_exemple5,rdd_exemple6,rdd_exemple7,rdd_exemple8,rdd_exemple9,rdd_exemple10,rdd_exemple11]
liste_nombre_de_sommet = []
liste_temps = []
liste_nb_composantes_connexe = []
liste_nb_iteration=[]

for rdd in liste_rdd:
  liste_nombre_de_sommet.append(rdd.union(rdd.map(lambda x : (x[1],x[0]))).groupByKey().count())
  temps = time.time()
  resultat,nb_iteration = result_algo(rdd)
  temps2 = time.time()
  x= temps2-temps
  liste_nb_iteration.append(nb_iteration)
  liste_nb_composantes_connexe.append(resultat.map(lambda x : (x[1],x[0])).groupByKey().count())
  liste_temps.append(x)


plt.xlabel("Nombre de sommets")
plt.ylabel("Temps d'execution")
plt.title("Variation du temps d'execution en fonction du nombre de sommets")
plt.plot(liste_nombre_de_sommet, liste_temps
plt.show()