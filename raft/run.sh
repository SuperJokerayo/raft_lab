echo "Running $1 times"
for _ in $(seq 1 $1) 
do  
for c in A B C D
do
time go test -run 2$c -race
done
time go test -run 2 -race
done 