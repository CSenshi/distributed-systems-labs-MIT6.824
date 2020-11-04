FILE=result.txt

# Test 2A
echo '' >${FILE}
for i in {1..100}; do
  echo "Retry ${i}"
  go test -race | tee -a ${FILE}
done
