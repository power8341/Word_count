## **Comparison Report: Hadoop vs. Spark Word Count Outputs**

### **1. Introduction**
Word count is a fundamental operation in big data processing, often used to compare frameworks like Hadoop and Spark. 
This report compares the word count results obtained from running the same input data using:
- **Hadoop (Java MapReduce)**
- **Apache Spark**

### **2. Output Analysis**
Below is a breakdown of the word count results.

#### **Hadoop Output**
(Extracted from the file `output_Hadoop`)
```
and	7
of	5
to	5
we	5
letters	3
that	3
a	3
them	3
the	3
poems	2
they	2
have	2
context	2
their	2
own	2
read	2
are	2
our	2
epistolary	2
it’s	2
```

#### **Spark Output**
(Extracted from the file `Output_spark.txt`)
```
and: 7
of: 5
to: 5
we: 5
letters: 3
that: 3
a: 3
them: 3
the: 3
poems: 2
they: 2
have: 2
context: 2
their: 2
own: 2
read: 2
are: 2
our: 2
epistolary: 2
it’s: 2
```

### **3. Observations**
- The word count results **match exactly** between Hadoop and Spark.
- The **formatting** differs:
  - Hadoop uses **tab-separated values (`word <tab> count`)**.
  - Spark uses **colon-separated values (`word: count`)**.
- The **actual counts of words are identical**, which confirms that both frameworks processed the same input data correctly.

### **4. Performance & Execution Comparison**
While the outputs are identical, the way Hadoop and Spark execute the word count operation is different:

| Feature  | Hadoop (Java MapReduce) | Apache Spark |
|----------|-----------------|--------------|
| **Execution Model** | Disk-based processing (writes intermediate results to HDFS) | In-memory processing (faster) |
| **Performance** | Slower due to disk I/O | Faster due to in-memory computations |
| **Ease of Development** | Requires more boilerplate Java code | More concise code using Scala/Python |
| **Scalability** | Scales well with large datasets | More efficient scaling due to DAG execution |

### **5. Conclusion**
- Both Hadoop and Spark produced **identical** word count results.
- Spark's **output formatting** differs slightly, using colons instead of tabs.
- Spark generally outperforms Hadoop in speed because of **in-memory computation**.
- Hadoop is more **disk-intensive** and can be **slower**, especially for iterative operations.
