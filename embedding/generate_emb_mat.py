import argparse
import torch
import os
import pickle

def main(data_dim, embed_dim):
    # Set a random seed for reproducibility
    torch.manual_seed(0)

    # Create a random projection matrix
    projection_matrix = torch.randn(embed_dim, data_dim)

    # Generate a unique filename
    file_index = 0
    filename = f"proj_mat/rand_proj_mat_modeldim={data_dim}_embeddim={embed_dim}.pkl"
    if os.path.exists(filename):
        print("CANNOT OVERWRITE FILE.")
        return -1

    # Save the matrix to a pickle file
    with open(filename, 'wb') as f:
        pickle.dump(projection_matrix, f)

    print(f"Random projection matrix saved to {filename}")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Random Projection Matrix Generator')
    parser.add_argument('--data_dim', type=int, required=True, help='Dimension of the data')
    parser.add_argument('--embed_dim', type=int, required=True, help='Dimension of the embedding')
    args = parser.parse_args()

    main(args.data_dim, args.embed_dim)
