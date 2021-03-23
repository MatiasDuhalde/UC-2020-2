import hashlib

def validate_proof_of_work(last_k, last_hash, k, end_hash):
    sha = hashlib.sha256(f'{last_k}{last_hash}{k}'.encode())
    return sha.hexdigest()[:len(end_hash)] == end_hash

def generate_proof_of_work(last_k, last_hash, end_hash):
    k = 0
    while not validate_proof_of_work(last_k, last_hash, k, end_hash):
        k += 1

    return k

if __name__ == "__main__":
    starting_sha = hashlib.sha256('0testString0'.encode())
    hash_string = starting_sha.hexdigest()
    # print(hash_string)

    starting_k = 0
    end_hash = '0000'

    out_k = generate_proof_of_work(starting_k, hash_string, end_hash)
    print(out_k)
