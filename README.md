RDFS
====

RDFS: an erasure code based cloud storage system

Our group has designed and implemented a new class of erasure codes based fault-tolerant distributed file system named Raid Distributed File System (RDFS).Novel features include the reliability, the high availability and the low storage overhead. We implemented a set of techniques to tackle the problems faced by integrating the erasure codes into the distributed file system:

(1) Parallel encoding/decoding: The encoding/decoding rates of erasure codes are significantly improved. Our experiments showed that the parallel optimized coding scheme is no longer the performance bottleneck in the system. The speedup rate of the parallel coding is usually bigger than three compared to the sequential coding schemes. 

(2) Network topology based tree-structured data reconstruction: Data are reconstructed periodically for decreasing the network costs. The tree-structured reconstruction decreases the usage of the network resources by 20%-30% compared to the traditional star-structured reconstruction. Further, the repairing rates are improved by 15%-20%.

(3) One to many reconstruction scheme for the simultaneous data losses: The data are repaired efficiently when multiple nodes fail. Compared to repairing individual nodes separately via the star-structured methods, the rates for the reconstruction are increased by about 30%. 

(4) Network topology based line-structured data reconstruction: The reading rates are optimized when a subset of blocks for the data fail during te reading process, i.e., partial losses. Compared to the traditional star-structured reconstruction, the reading rates are increased by 50%-100% for the partial losses.

The above techniques do not require specific coding schemes and thus are quite general. RDFS works quite well with most of the existing erasure codes as its underlying coding schemes.



  Contact: xiaoqiangpei@nudt.edu.cn, xuflnk@gmail.com, wwyyjj1971@vip.sina.com, quanyongf@gmail.com

