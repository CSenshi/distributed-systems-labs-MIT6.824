FILE_A=result_a.txt

# Test 2A
echo '' >${FILE_A}
for i in {1..100}; do
  echo "Retry ${i}"
  go test -run 2A -race | tee -a ${FILE_A}
done
