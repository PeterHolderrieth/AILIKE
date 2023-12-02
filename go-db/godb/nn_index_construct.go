package godb

import (
	"fmt"
	"os"
)

func ConstructIndexFileFromHeapFile(hfile *HeapFile, indexedColName string, nClusters int, DataFile string, CentroidFile string, bp *BufferPool) (*NNIndexFile, error) {

	//Create clustering
	getterFunc := GetSimpleGetterFunc(indexedColName)
	clustering, err := KMeansClustering(hfile, nClusters, TextEmbeddingDim,
		MaxIterKMeans, DeltaThrKMeans, getterFunc, false)
	if err != nil {
		return nil, err
	}

	//Create data files
	err = os.Remove(DataFile)
	if err != nil {
		fmt.Println("Error data file.")
		return nil, err
	}
	dataHeapFile, err := NewHeapFile(DataFile, &dataDesc, bp)
	if err != nil {
		return nil, err
	}

	//Create centroid files
	err = os.Remove(CentroidFile)
	if err != nil {
		fmt.Println("Error removing centroid file.")
		return nil, err
	}
	centroidHeapFile, err := NewHeapFile(CentroidFile, &centroidDesc, bp)
	if err != nil {
		return nil, err
	}

	//Insert all centroids and elements into the data file

	//Insert all centroids and elements into the data file

	return &NNIndexFile{hfile.fileName, indexedColName, dataHeapFile, centroidHeapFile}, nil
}
