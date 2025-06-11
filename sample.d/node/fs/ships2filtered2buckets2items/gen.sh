#!/bin/sh

gorder(){
  order=$1
  user=$2
  unixtime_ms=$3

  jq \
    -c \
    -n \
    --arg order $order \
    --arg user $user \
    --arg unixtime_ms $unixtime_ms \
    '{
      order: $order | tonumber,
      user: $user | tonumber,
      unixtime_ms: $unixtime_ms | tonumber,
      items: [
        {price: 42.195, quantity: 634, id: 3776},
        {price:  3.776, quantity: 333, id:  599}
      ]
    }'
}

geninput(){
  echo creating input files...

  mkdir -p ./sample.d

  printf \
    '%s\n' \
    cafef00d-dead-beaf-face-864299792458/2025/06/10 \
    cafef00d-dead-beaf-face-864299792458/2025/06/11 \
    cafef00d-dead-beaf-face-864299792458/2025/06/12 \
    dafef00d-dead-beaf-face-864299792458/2025/06/10 \
    dafef00d-dead-beaf-face-864299792458/2025/06/11 \
    dafef00d-dead-beaf-face-864299792458/2025/06/12 |
    while read line; do
      bname=$( basename "${line}" )
      dname="./sample.d/${line}"
      sleep 0.5
      for bucket in 4093 2047 1023; do
        oname="./sample.d/${line}/${bucket}.jsonl"
        for user in 2025 611; do
          for orderbase in 1 2 3; do
            order=$(( $bname << 32 | $user << 16 | $orderbase ))
            unixtime_ms="$( date +%s )000"
            xord=$(( ${order} ^ ${bucket} ^ ${unixtime_ms} ))
            gorder $xord $user $unixtime_ms
          done
        done |
          jq -c |
          cat > "${oname}"
      done
    done
}

geninput
