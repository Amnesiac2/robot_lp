# -*- coding: utf-8 -*-

'''
利用MapReduce实现K_means算法
'''

from numpy import *


def dist(vecA, vecB):    # 计算两向量欧式距离
    return sqrt(sum(pow(vecA - vecB, 2)))


def randCentor(dataSet, k):   # 随机构建簇中心
    n = shape(dataSet)[1]
    cent = mat(zeros((k, n)))
    for j in range(n):
        minJ = min(dataSet[:, j])
        rangeJ = float(max(dataSet[:, j]) - minJ)
        cent[:, j] = minJ + rangeJ * random.rand(k, 1)
    return cent


def Kmeans(dataSet, k):
    m = shape(dataSet)[0]
    clusterAssment = mat(zeros((m, 2)))   # 存储簇索引值以及到簇中心的距离
    cent = randCentor(dataSet, k)
    # 伪代码：将cent信息存入本地文件，在启动mapreduce过程后将cent文件分发到各个节点。
    clusterChanged = True
    while clusterChanged:
        clusterChanged = False
        # 伪代码：map过程：将数据分成N份到各个节点，与cent文件中的簇中心计算距离，输出类别和数据。
        for i in range(m):
            minDist = inf
            minIndex = -1
            for j in range(k):
                distJI = dist(cent[j, :], dataSet[i, :]) # 计算当前点到各簇中心的距离
                if distJI < minDist:
                    minDist = distJI
                    minIndex = j
            if clusterAssment[i, 0] != minIndex:   # 直到所有数据点的簇分配结果不再改变后停止循环
                clusterChanged = True
            clusterAssment[i, :] = minIndex, minDist ** 2   # 得到各点所属的簇和与簇中心的距离
        print cent
        # 伪代码：reduce过程：将各数据的分类情况汇总到本地，将同一类别的数据相加求平均计算簇中心。
        for centor in range(k):
            dataInClust = dataSet[nonzero(clusterAssment[:, 0].A == cent)[0]]
            cent[centor, :] = mean(dataInClust, axis = 0)
    return cent, clusterAssment



