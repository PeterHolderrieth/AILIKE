package godb

//DEBUG:
//I DO NOT WANT TO STORE THE EMBEDDING VECTORS IN A MAPPING AS WE DO NOT WANT TO ASSUME THAT THEY
//ARE STORED IN MEMORY
import (
	"fmt"
	"math"
)

const (
	DefaultDeltaThreshold       = float64(0.01)
	DefaultIterationThreshold   = 12
	DefaultClusterTransactionID = 0
)

type ClusterMember struct {
	rid recordID
	Emb *EmbeddingType
}

type Clustering struct {
	embDim         int
	centroidEmbs   map[int]*EmbeddingType
	clusterMemb    map[int][]ClusterMember
	sumClusterDist map[int]float64
	maxNClusters   int //Desired number of clusters. Actual number of clusters might be less if number of data points is less
	distFunc       func(e1, e2 *EmbeddingType) float64
}

func MSEDistance(e1, e2 *EmbeddingType) float64 {

	sum_dist := float64(0.0)
	for idx, e1_idx := range *e1 {
		e2_idx := (*e2)[idx]
		sum_dist += (e1_idx - e2_idx) * (e1_idx - e2_idx)
	}
	return sum_dist / float64(len(*e1))
}

func newClustering(nClusters, embDim int) *Clustering {
	return &Clustering{centroidEmbs: make(map[int]*EmbeddingType),
		clusterMemb:    make(map[int][]ClusterMember),
		sumClusterDist: make(map[int]float64),
		maxNClusters:   nClusters,
		embDim:         embDim,
		distFunc:       MSEDistance, //MSE distance by default at the moment
	}
}

func (c *Clustering) NCentroids() int {
	return len(c.centroidEmbs)
}

func (c *Clustering) TotalDist() float64 {
	sumDist := float64(0.0)
	for _, dist := range c.sumClusterDist {
		sumDist += dist
	}
	return sumDist
}

func (c *Clustering) Print() {
	for key, x := range c.centroidEmbs {
		fmt.Println("Cluster ID: ", key, " | ", *x)
		fmt.Println("Members: ")
		for _, z := range c.clusterMemb[key] {
			fmt.Println(z.rid, " | ", *z.Emb)
		}
		fmt.Println("Cluster quality: ", c.sumClusterDist[key])
	}
	fmt.Println("Total distance to centroids: ", c.TotalDist())
	fmt.Println("")
}

// Function to find closest centroid in clustering
func (c *Clustering) FindClosestCentroid(emb *EmbeddingType) (int, float64) {

	minDistToCentroid := math.MaxFloat64
	centroidAssignment := -1
	var distToCentroid float64
	for idx, centrEmb := range c.centroidEmbs {
		distToCentroid = c.distFunc(emb, centrEmb)
		if distToCentroid < minDistToCentroid {
			minDistToCentroid = distToCentroid
			centroidAssignment = idx
		}
	}
	return centroidAssignment, minDistToCentroid
}

// Function to add an embedding to a cluster
// - creates new centroid if there is an empty cluster
// - otherwise finds the closest centroid and adds it
func (c *Clustering) addRecordToClustering(rid recordID, emb *EmbeddingType) error {

	newMember := ClusterMember{Emb: emb, rid: rid}
	if c.NCentroids() < c.maxNClusters {
		newCentroidId := c.NCentroids()
		members := make([]ClusterMember, 1)
		members[0] = newMember
		//Get number of members
		if _, ok := c.centroidEmbs[newCentroidId]; ok {
			return GoDBError{errString: "Expected centroid to not exist but it exists.",
				code: UnknownClusterError}
		}
		c.centroidEmbs[newCentroidId] = emb
		c.clusterMemb[newCentroidId] = members
		c.sumClusterDist[newCentroidId] = 0.0
		return nil

	} else {
		centroidAssignment, minDistToCentroid := c.FindClosestCentroid(emb)
		c.clusterMemb[centroidAssignment] = append(c.clusterMemb[centroidAssignment],
			newMember)
		c.sumClusterDist[centroidAssignment] += minDistToCentroid
		return nil
	}
}

func (c *Clustering) updateSingleCentroidVector(clusterID int) error {

	//Get number of members
	if _, ok := c.clusterMemb[clusterID]; !ok {
		return GoDBError{errString: "Do not know this clusterID.", code: UnknownClusterError}
	}
	nMembers := len(c.clusterMemb[clusterID])

	meanEmb := make(EmbeddingType, c.embDim)

	// Go over all cluster members and compute average:
	for _, clusterMember := range c.clusterMemb[clusterID] {
		for i := 0; i < c.embDim; i++ {
			meanEmb[i] += (*clusterMember.Emb)[i] / float64(nMembers)
		}
	}
	c.centroidEmbs[clusterID] = &meanEmb

	// Go over all cluster members and compute average distance to new centroid:
	sumDist := float64(0.0)
	for _, clusterMember := range c.clusterMemb[clusterID] {
		sumDist += c.distFunc(&meanEmb, clusterMember.Emb)
	}
	c.sumClusterDist[clusterID] = sumDist

	return nil
}

func (c *Clustering) updateAllCentroidVectors() error {
	for clusterID := range c.centroidEmbs {
		err := c.updateSingleCentroidVector(clusterID)
		if err != nil {
			return err
		}
	}
	return nil
}

// Function to delete members from cluster (while keeping the centroids)
func (c *Clustering) deleteAllMembers() error {
	for clusterID := range c.clusterMemb {
		c.clusterMemb[clusterID] = make([]ClusterMember, 0)
		c.sumClusterDist[clusterID] = float64(0.0)
	}
	return nil
}

func KMeansClustering(op Operator, nClusters int, embDim int,
	maxIterations int, deltaThr float64,
	embGetterFunc func(t *Tuple) (*EmbeddingType, error)) (*Clustering, error) {

	clustering := newClustering(nClusters, embDim)

	nIteration := 0
	//currError := deltaThr

	for (nIteration < maxIterations) && (true) {
		//Renew iterator
		iterator, err := op.Iterator(NewTID())
		if err != nil {
			return nil, err
		}

		// Deleting all members (as they are about to be readded)
		err = clustering.deleteAllMembers()
		if err != nil {
			return nil, err
		}

		// Go over iterator and add all new elements
		for newTuple, err := iterator(); (err != nil) || (newTuple != nil); newTuple, err = iterator() {
			if err != nil {
				return nil, err
			}
			newEmb, err := embGetterFunc(newTuple)
			if err != nil {
				return nil, err
			}
			clustering.addRecordToClustering((*newTuple).Rid, newEmb)
		}

		// Update all centroid vectors
		clustering.updateAllCentroidVectors()

		nIteration += 1
	}

	return clustering, nil
}

//Function to reassign elements from an iterator
// - Could be heapFile iterator
// - Or could be Clustering iterator

//Function to add elements/tuples from heapFile iterator into clustering

//How do I avoid re-insertion???
