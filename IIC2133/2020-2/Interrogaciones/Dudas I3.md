# Sesiones de dudas (Zoom)

* Sesión 1: https://zoom.us/rec/share/CfCwOUGGbd-nHnCx8OhMobrjlKKLPVaSLFHo1j76HpIp2XSCCuKT67Nx2aAl6cqW.hUcH6i1rfcGUVksP
* Sesión 2: [Video]
* Sesión 3: [Video]

# Recopilado de dudas

## Pregunta 1

* Asume que la ruta es una línea recta
* No importa _quien_ ve el letrero, el w_i representa la ganancia del letrero sub i
* k es una constante
* Debes responder la parte A y B. No se pide que resuelvas el problema.

### Parte A

---

### Parte B

---

## Pregunta 2

* La definición algoritmo la vimos al principio del curso. Pueden hacerlo en prosa o en pseudocódigo, o una mezcla de ambos.
* Puedes asumir que el par (a,a) siempre va a aparecer para cada persona a
* Solo existen las personas que aparecen en los pares de F o U
* Puedes asumir que si está (a,b) entonces tambien está (a,a), (b,b) y (b,a)
* Las personas de un grupo de amigos no son necesariamente todas amigas entre ellas
* No conoces de antemano cuantas personas hay

### Parte A

* Lineal = O(|F|)

### Parte B

* Puedes asumir que los trios que vienen en U siempre te permitirán resolver el problema
* una función linearitmica es una función lineal multiplicada por una función logaritmica. En terminos de O, O(n log m), donde m puede o no ser igual a n
* A lo más linearitmico significa que en el peor caso puede ser linearítmico. Es decir, la complejidad se pide en términos de O
* Si tenemos (a,b,w), entonces el trio (b,a,w) tendría el mismo w
* Una vez que a y b se hacen amigos, entonces b tambien es amigo de a
* La lista U no viene ordenada en ningun orden particular

## Pregunta 3

* f(v) es el NUMERO de aristas de la ruta de s a v, y es independiente del costo de la ruta
* f(v) es una propiedad de un vértice independiente de que hayas calculado o no las rutas
* Puedes asumir la representación del grafo que más te acomode
* No puedes usar f(v), g(v) o L como un valor conocido; si lo necesitas, calculalo, pero ojo con la complejidad que eso signifique.
* HINT: Para resolver este problema es importante considerar de donde viene la complejidad del algoritmo, y ver que estamos haciendo o dejando de hacer para obtener estas nuevas complejidades. En particular, debes identificar que aristas no es necesario revisar en algun momento del algoritmo.

### Parte A

* L es una propiedad del grafo
* L no es conocido de antemano
* Lo unico que importa es que la complejidad sea theta(LE), independiente de si conocemos L o no

### Parte B

* Esta sumatoria es en el peor caso O(LE), es decir, mejor que la complejidad de la parte A
* El f y el g no son funciones que nosotros podamos llamar; son propiedades del grafo. Si las necesitas, calculalas, pero ojo con la complejidad que eso signifique.

## Pregunta 4

* El árbol no necesariamente es completo, puede estar absolutamente desbalanceado.
* Puedes asumir que cada regalo tiene un identificador único, y que son todos distintos entre ellos.
* Lo unico que tienes es un puntero a la raíz. Si quieres encontrar las hojas debes recorrerlo hacia abajo hasta llegar a un vértice que no tenga hijos.
* La estructura del árbol es fija.
* Si escoges un regalo a profundidad P, entonces ya estás escogiendo P regalos.
* La restriccion de los padres no aplica hacia abajo: si escoges un regalo U puedes o no escoger los hijos de U (uno o ambos)
* La altura del árbol no es un parámetro conocido.
* El árbol viene de la forma que les convenga: lo unico que representa el árbol es que existe una relación padre-hijo entre regalos, y que cada regalo puede tener como máximo dos hijos.
* El algoritmo puede ser top-down o bottom-up, lo que más les convenga.
* n >= k
