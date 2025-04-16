BUCKET="s3tar"
REGION="us-east-1"

SRC="s3://$BUCKET/archive/olx/olx_s/Account/year=2022/month=10/day=10.tar"
DST="s3://$BUCKET/"

echo "===========Generating TOC==========="
s3tar --region "$REGION" \
  --generate-toc \
  -f "$SRC" \
  -C "toc.csv"

echo "==============Extraindo $SRC → $DST================"
s3tar --region "$REGION" \
  --external-toc "toc.csv" \
  -xvf "$SRC" \
  -C "$DST" \
  
