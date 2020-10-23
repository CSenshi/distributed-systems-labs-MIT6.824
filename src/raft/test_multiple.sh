FILE=result.txt

# Test 2A
echo '' >${FILE}
for i in {1..100}; do
  echo "Retry ${i}"
  go test -run '(2A|2B)' -race | tee -a ${FILE}
done
