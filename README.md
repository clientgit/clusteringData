[(расстояния измеряются в метрике Евклида)]: <https://wiki.loginom.ru/articles/euclid-distance.html>
[Euclidean distance]: <https://wiki.loginom.ru/articles/euclid-distance.html>
[stackoverflow.csv (170 МБ)]:  <https://moocs.scala-lang.org/~dockermoocs/bigdata/stackoverflow.csv>
#### Grouping StackOverflow Q&A platform data with Spark for analysis.
__________________________________________________________
В этом задании алгоритм k-means группирует вопросы и ответы с платформы StackOverflow в соответствии с их оценкой.
Расчет может быть запущен как локально, так и паралельно на Worked Node with Spark Executors.  

![](https://www.bigdataschool.ru/wp-content/uploads/2021/04/sparch1.png)
##### For run:
__________________________________________________________
1. Clone this project
2. Importing an sbt project into IntelliJ
    + File menu -> New -> Project from Existing Sources...
    + Select a file that contains your sbt project description build.sbt. Click OK.

3. Download data - [stackoverflow.csv (170 МБ)] and place it in the folder:
   -`src/main/resources/stackoverflow` in your project directory
3. **_Run_** `src/main/scala/stackoverflow/StackOverflow.scala`

##### Sample output:
__________________________________________________________
```
Resulting clusters:
  maxScore  medianScore  Dominant language (%percent)  Questions
================================================================
      {num}       {num}  someLang          ({num}%)        {num}        // cluter 1
       ...         ...   ...                 ...            ...                 
       ...         ...   ...                 ...            ...
       ...         ...   ...                 ...            ...
      {num}       {num}  someLang          ({num}%)        {num}        // cluter 45
```
###### If you **Run** `-> def kmeans(means, vectors, iter=1, debug)` where **_debug=true_**, then the [Euclidean distance] for each iteration will be displayed
```
Iteration: 29
  * current distance: 31273.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0      // cluster 1
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2179) ==>             (0,2203)    distance:      576      // cluster 4
                    ...                      ...                   ...
           (250000,145) ==>         (250000,156)    distance:      121      // cluster 42 
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0      // cluster 45
```

##### K-means algorithm theory mapped to code:
__________________________________________________________
Метод k-средних используется для кластеризации данных на основе алгоритма разбиения векторного пространства на заранее определенное число кластеров k. Алгоритм представляет собой итерационную процедуру, в которой выполняются следующие шаги:

1. Выбирается число кластеров k:
   `-> def kmeansKernels: Int = ...`
2. Из исходного множества данных случайным образом выбираются k наблюдений (**A, B, C**), которые будут служить начальными центрами кластеров.
   `->  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = ...`

   ![](https://infostart.ru/upload/iblock/1cb/%D0%A0%D0%B8%D1%81%D1%83%D0%BD%D0%BE%D0%BA%201.PNG)

3. Для каждого наблюдения исходного множества определяется ближайший к нему центр кластера [(расстояния измеряются в метрике Евклида)]. При этом записи, «притянутые» определенным центром, образуют начальные кластеры:
   `-> def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = ...`

4. Вычисляются центроиды — центры тяжести кластеров. Каждый центроид — это вектор, элементы которого представляют собой средние значения соответствующих признаков, вычисленные по всем записям кластера:  
   `-> def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = ... `
5. Центр кластера смещается в его центроид, после чего центроид становится центром нового кластера. 

   ![](https://infostart.ru/upload/iblock/1a0/%D0%A0%D0%B8%D1%81%D1%83%D0%BD%D0%BE%D0%BA%202.PNG)
6. 3-й и 4-й шаги итеративно повторяются. Очевидно, что на каждой итерации происходит изменение границ кластеров и смещение их центров. В результате минимизируется расстояние между элементами внутри кластеров и увеличиваются междукластерные расстояния:  
   `-> @tailrec final def kmeans(means, vectors, iter=1, debug=false): Array[(Int, Int)] = ...  `

Остановка алгоритма производится тогда, когда границы кластеров и расположения центроидов не перестанут изменяться от итерации к итерации, т.е. на каждой итерации в каждом кластере будет оставаться один и тот же набор наблюдений: 
`-> def converged(distance: Double): Boolean =`

#### Some code description:
__________________________________________________________
```
val lines   = sc.textFile("pathFile")    // строки из CSV-файла
val raw     = rawPostings(lines)         // необработанные записи для каждой строки
val grouped = groupedPostings(raw)       // сгруппированы вместе соответствующие вопросы и ответы 
val scored  = scoredPostings(grouped)    // вопросы и максимальный балл за каждый вопрос 
val vectors = vectorPostings(scored)     // пары (индекс языка, оценка) для каждого вопроса

/*
 * Центры тяжести для каждого кластера. 
 * Для итераций используется хвостовая рекурсия, которая на каждой итерации сопоставляет 
 * каждый вектор с индексом ближайшего среднего кластера (def findClosest), и
 * вычисляет новые средние значения путем усреднения значений клаждого кластера (averageVectors).
 */
val means = kmeans(sampleVectors(vectors), vectors, debug = true)

/*
 * Переменная langSpread соответствует тому, насколько далеко находятся языки
 * с точки зрения алгоритма кластеризации. При значении 50000 языки находятся 
 * слишком далеко друг от друга, чтобы их можно было сгруппировать вместе,
 * это приводит к кластеризации, в которой учитываются только оценки для каждого языка.
 */
def langSpread = 50000

/*
 * Вычисленная информация о каждом кластере: 
 *   - доминирующий язык программирования в кластере 
 *   - процент ответов, относящихся к доминирующему языку
 *   - размер кластера (количество содержащихся в нем вопросов)
 *   - медиана наивысших баллов ответов
 */
val results = clusterResults(means, vectors) 
```