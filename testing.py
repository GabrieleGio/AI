import matplotlib.pyplot as plt
import numpy as np

x = np.random.rand(10000)

plt.hist(x, bins=50, color='skyblue', edgecolor='black')
plt.title('Istogramma di numeri casuali')
plt.xlabel('Valori')
plt.ylabel('Frequenza')
plt.show()



