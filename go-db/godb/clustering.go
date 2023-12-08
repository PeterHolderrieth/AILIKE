package godb

import (
	"fmt"
	"math"
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
	distFunc       func(e1, e2 *EmbeddingType) (float64, error)
	storeEmbs      bool
}

func newClustering(nClusters, embDim int, storeEmbs bool) *Clustering {
	return &Clustering{centroidEmbs: make(map[int]*EmbeddingType),
		clusterMemb:    make(map[int][]ClusterMember),
		sumClusterDist: make(map[int]float64),
		maxNClusters:   nClusters,
		embDim:         embDim,
		distFunc:       NegativeDotProduct,
		storeEmbs:      storeEmbs,
	}
}

// Return number of clusters/centroids
func (c *Clustering) NCentroids() int {
	return len(c.centroidEmbs)
}

// Return total number of members in clustering
func (c *Clustering) TotalNMembers() int {
	n_members := 0
	for _, x := range c.clusterMemb {
		n_members += len(x)
	}
	return n_members
}

// Return the sum of all within-cluster distances
// This gives a way of getting the relative quality of a clustering
func (c *Clustering) TotalDist() float64 {
	sumDist := float64(0.0)
	for _, dist := range c.sumClusterDist {
		sumDist += dist
	}
	return sumDist
}

// Print clustering (it also prints) the cluster member embeddings.
// So it should only be used for debugging (and with embDim < 10).
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
func (c *Clustering) FindClosestCentroid(emb *EmbeddingType) (int, float64, error) {

	minDistToCentroid := math.MaxFloat64
	centroidAssignment := -1
	var distToCentroid float64
	var err error
	for idx, centrEmb := range c.centroidEmbs {
		distToCentroid, err = c.distFunc(emb, centrEmb)
		if err != nil {
			return 0, 0.0, err
		}
		if distToCentroid < minDistToCentroid {
			minDistToCentroid = distToCentroid
			centroidAssignment = idx
		}
	}
	return centroidAssignment, minDistToCentroid, nil
}

// Function to add an embedding to a cluster
// - creates new centroid if there is an empty cluster
// - otherwise finds the closest centroid and adds it
func (c *Clustering) addRecordToClustering(rid recordID, emb *EmbeddingType) (int, float64, error) {

	//Create new embedding
	newMember := ClusterMember{Emb: nil, rid: rid}
	if c.storeEmbs {
		newMember.Emb = emb
	}

	//If there is an empty centroid, simply create it
	if c.NCentroids() < c.maxNClusters {

		//Add record itself to the members
		newCentroidId := c.NCentroids()
		members := make([]ClusterMember, 1)
		members[0] = newMember
		if _, ok := c.centroidEmbs[newCentroidId]; ok {
			return 0, 0.0, GoDBError{errString: "Expected centroid to not exist but it exists.",
				code: UnknownClusterError}
		}
		c.centroidEmbs[newCentroidId] = emb

		//Make this record's embedding the centroid:
		c.clusterMemb[newCentroidId] = members

		//Init distance to zero
		c.sumClusterDist[newCentroidId] = 0.0

		return newCentroidId, 0.0, nil

	} else {
		//Find closest centroid and then add this record:
		centroidAssignment, minDistToCentroid, err := c.FindClosestCentroid(emb)
		if err != nil {
			return 0, 0.0, nil
		}
		c.clusterMemb[centroidAssignment] = append(c.clusterMemb[centroidAssignment], newMember)
		c.sumClusterDist[centroidAssignment] += minDistToCentroid
		return centroidAssignment, minDistToCentroid, nil
	}
}

// Updating a single centroid:
// - Can be either done manually (by specific newEmb and newSumDist)
// - or automatic: by computing the mean over all cluster member embeddings
func (c *Clustering) updateSingleCentroidVector(clusterID int, newEmb *EmbeddingType, newSumDist *float64) error {

	//Check if cluster exists
	if _, ok := c.clusterMemb[clusterID]; !ok {
		return GoDBError{errString: "Do not know this clusterID.", code: UnknownClusterError}
	}

	//If an embedding is given, then simply insert it
	if newEmb != nil {
		if newSumDist == nil {
			return GoDBError{errString: "If newEmb is given, also newSumDist must be given.", code: TypeMismatchError}
		}
		c.centroidEmbs[clusterID] = newEmb
		c.sumClusterDist[clusterID] = *newSumDist

		//Otherwise, perform manual update
	} else {
		if !c.storeEmbs {
			return GoDBError{errString: "Embeddings can only be automatically updated if c.storeEmbs is true (need to store embeddings).", code: TypeMismatchError}
		}

		if newSumDist != nil {
			return GoDBError{errString: "If a newSumDist is given, also newEmb must be given.", code: TypeMismatchError}
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
			newDist, err := c.distFunc(&meanEmb, clusterMember.Emb)
			sumDist += newDist
			if err != nil {
				return err
			}
		}
		c.sumClusterDist[clusterID] = sumDist
	}
	return nil
}

func (c *Clustering) updateAutomaticAllCentroidVectors() error {
	for clusterID := range c.centroidEmbs {
		err := c.updateSingleCentroidVector(clusterID, nil, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Clustering) updateManualAllCentroidVectors(CentroidMap map[int]*EmbeddingType, distMap map[int]*float64) error {
	for clusterID := range c.centroidEmbs {
		err := c.updateSingleCentroidVector(clusterID, CentroidMap[clusterID], distMap[clusterID])
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
	embGetterFunc func(t *Tuple) (*EmbeddingType, error),
	storeEmbs bool) (*Clustering, error) {

	clustering := newClustering(nClusters, embDim, storeEmbs)
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
		CentroidMap := make(map[int]*EmbeddingType)
		distMap := make(map[int]*float64)

		// Go over iterator and add all new elements
		for newTuple, err := iterator(); (err != nil) || (newTuple != nil); newTuple, err = iterator() {
			if err != nil {
				return nil, err
			}
			newEmb, err := embGetterFunc(newTuple)
			if err != nil {
				return nil, err
			}
			clusterAssignment, distToCentroid, err := clustering.addRecordToClustering((*newTuple).Rid, newEmb)
			if err != nil {
				return nil, err
			}
			//Update distance map
			if _, ok := distMap[clusterAssignment]; !ok {
				zeroVal := float64(0.0)
				distMap[clusterAssignment] = &zeroVal
			}
			newDist := (*distMap[clusterAssignment] + distToCentroid)
			distMap[clusterAssignment] = &newDist

			//Update centroid map
			if _, ok := CentroidMap[clusterAssignment]; !ok {
				newCentroid := make(EmbeddingType, clustering.embDim)
				CentroidMap[clusterAssignment] = &newCentroid
			}
			NewSum := *CentroidMap[clusterAssignment]
			for i := 0; i < clustering.embDim; i++ {
				NewSum[i] += (*newEmb)[i]
			}
			CentroidMap[clusterAssignment] = &NewSum

		}
		//Convert sum of all embedding vectors to mean
		for clusterID, members := range clustering.clusterMemb {
			nMembers := len(members)
			if _, ok := CentroidMap[clusterID]; !ok {
				if nMembers != 0 {
					return nil, GoDBError{errString: "The centroid map does not have that cluster registered but the cluster has more than 0 members.",
						code: UnknownClusterError}
				}
				CentroidMap[clusterID] = nil
				distMap[clusterID] = nil
			} else {
				NewMean := *CentroidMap[clusterID]
				for i := 0; i < clustering.embDim; i++ {
					NewMean[i] = NewMean[i] / float64(nMembers)
				}
				CentroidMap[clusterID] = &NewMean
			}
		}

		// Update all centroid vectors
		clustering.updateManualAllCentroidVectors(CentroidMap, distMap)

		nIteration += 1
	}
	return clustering, nil
}
