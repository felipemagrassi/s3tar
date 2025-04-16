#!/bin/bash

BUCKET="s3tar"
REGION="us-east-1"
COMPANY="b/b_short"
COMPANY_SHORT=$(echo "$COMPANY" | sed 's/\//_/g')
ROOT_PREFIX="raw/$COMPANY"
ARCHIVE_ROOT="archive/$COMPANY"

HOJE=$(date +%s)
SHOULD_DELETE=false
DRY_RUN=false
PROFILE_ARG=""
PROCESSED_FILE="processed_paths_${COMPANY_SHORT}.txt"
DEBUG=true

# Função para calcular timestamp Unix a partir de YYYY-MM-DD
# Compatível com Linux e macOS
get_date_timestamp() {
  local date_str="$1"
  
  if [[ "$(uname)" == "Darwin" ]]; then
    # macOS usa date diferente
    date -j -f "%Y-%m-%d" "$date_str" "+%s" 2>/dev/null || echo 0
  else
    # Linux
    date -d "$date_str" "+%s" 2>/dev/null || echo 0
  fi
}

log_debug() {
  if [ "$DEBUG" = true ]; then
    echo "🔍 DEBUG: $1"
  fi
}

log_info() {
  echo "ℹ️ INFO: $1"
}

log_success() {
  echo "✅ SUCESSO: $1"
}

log_warning() {
  echo "⚠️ AVISO: $1"
}

log_error() {
  echo "❌ ERRO: $1"
}

log_dry_run() {
  echo "🔬 DRY-RUN: $1"
}

for arg in "$@"; do
  case $arg in
    --delete)
      SHOULD_DELETE=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --profile=*)
      PROFILE_NAME="${arg#*=}"
      PROFILE_ARG="--profile $PROFILE_NAME"
      shift
      ;;
    *)
      ;;
  esac
done

if [ "$DRY_RUN" = true ]; then
  PROCESSED_FILE="processed_paths_${COMPANY_SHORT}_dry_run.txt"
  log_info "Modo DRY-RUN ativado - usando arquivo de controle: $PROCESSED_FILE"
fi

log_info "Processando empresa: $COMPANY (arquivo de controle: $PROCESSED_FILE)"

touch "$PROCESSED_FILE"

log_info "Iniciando processamento com bucket=$BUCKET, região=$REGION, prefixo=$ROOT_PREFIX"

aws $PROFILE_ARG s3 ls "s3://$BUCKET/$ROOT_PREFIX/" | awk '/PRE/ {print $2}' | tr -d '/' | while read -r OBJECT; do
  
  if grep -q "^$COMPANY_SHORT:$OBJECT:ALL$" "$PROCESSED_FILE"; then
    log_debug "Pulando objeto $OBJECT para empresa $COMPANY (já processado)"
    continue
  fi
  
  log_info "Processando objeto: $OBJECT para empresa $COMPANY"
  BASE_PREFIX="$ROOT_PREFIX/$OBJECT"
  ARCHIVE_PREFIX="$ARCHIVE_ROOT/$OBJECT"
  
  OBJECT_YEARS=$(aws $PROFILE_ARG s3 ls "s3://$BUCKET/$BASE_PREFIX/" | awk '/PRE year=/ {print $2}' | tr -d '/' | sort)
  
  for YEAR_DIR in $OBJECT_YEARS; do
    YEAR=$(echo "$YEAR_DIR" | awk -F= '{print $2}')
    
    if grep -q "^$COMPANY_SHORT:$OBJECT:$YEAR:ALL$" "$PROCESSED_FILE"; then
      log_debug "Pulando ano $YEAR para $OBJECT/$COMPANY (já processado)"
      continue
    fi
    
    log_debug "Verificando ano: $YEAR para $OBJECT/$COMPANY"
    YEAR_PROCESSED=false
    
    OBJECT_MONTHS=$(aws $PROFILE_ARG s3 ls "s3://$BUCKET/$BASE_PREFIX/year=$YEAR/" | awk '/PRE month=/ {print $2}' | tr -d '/' | sort)
    
    for MONTH_DIR in $OBJECT_MONTHS; do
      MONTH=$(echo "$MONTH_DIR" | awk -F= '{print $2}')
      
      if grep -q "^$COMPANY_SHORT:$OBJECT:$YEAR:$MONTH:ALL$" "$PROCESSED_FILE"; then
        log_debug "Pulando mês $YEAR-$MONTH para $OBJECT/$COMPANY (já processado)"
        continue
      fi
      
      log_debug "Verificando mês: $MONTH/$YEAR para $OBJECT/$COMPANY"
      MONTH_PROCESSED=false
      
      OBJECT_DAYS=$(aws $PROFILE_ARG s3 ls "s3://$BUCKET/$BASE_PREFIX/year=$YEAR/month=$MONTH/" | awk '/PRE day=/ {print $2}' | tr -d '/' | sort)
      
      for DAY_DIR in $OBJECT_DAYS; do
        DAY=$(echo "$DAY_DIR" | awk -F= '{print $2}')
        
        PATH_KEY="$COMPANY_SHORT:$OBJECT:$YEAR:$MONTH:$DAY"
        if grep -q "^$PATH_KEY$" "$PROCESSED_FILE" || grep -q "^$PATH_KEY:IGNORED_RULE_1$" "$PROCESSED_FILE" || grep -q "^$PATH_KEY:IGNORED_RULE_90$" "$PROCESSED_FILE" || grep -q "^$PATH_KEY:EMPTY$" "$PROCESSED_FILE"; then
          log_debug "Pulando $YEAR-$MONTH-$DAY para $OBJECT/$COMPANY (já processado)"
          continue
        fi
        
        if [ "$DAY" -eq 1 ]; then
          log_debug "Ignorando $YEAR-$MONTH-$DAY para $OBJECT/$COMPANY (dia 1)"
          echo "$PATH_KEY:IGNORED_RULE_1" >> "$PROCESSED_FILE"
          continue
        fi
        
        DATA_DIR=$(get_date_timestamp "$YEAR-$MONTH-$DAY")
        DIFF=$(( (HOJE - DATA_DIR) / 86400 ))
        
        if [ "$DATA_DIR" -eq 0 ] || [ "$DIFF" -lt 90 ]; then
          log_debug "Ignorando $YEAR-$MONTH-$DAY para $OBJECT/$COMPANY (só $DIFF dias)"
          echo "$PATH_KEY:IGNORED_RULE_90" >> "$PROCESSED_FILE"
          continue
        fi
        
        SRC="s3://$BUCKET/$BASE_PREFIX/year=$YEAR/month=$MONTH/day=$DAY/"
        DST="s3://$BUCKET/$ARCHIVE_PREFIX/year=$YEAR/month=$MONTH/day=$DAY.tar"
        log_debug "Verificando objetos em $SRC"
        
        OBJECT_COUNT=$(aws s3api list-objects-v2 \
          --bucket "$BUCKET" \
          --prefix "${BASE_PREFIX}/year=$YEAR/month=$MONTH/day=$DAY/" \
          --query "length(Contents)" \
          --output text \
          $PROFILE_ARG)
        
        if [ "$OBJECT_COUNT" == "None" ] || [ "$OBJECT_COUNT" -lt 2 ]; then
          log_warning "Ignorando $SRC (sem arquivos reais: $OBJECT_COUNT)"
          echo "$PATH_KEY:EMPTY" >> "$PROCESSED_FILE"
          MONTH_PROCESSED=true
          continue
        fi
        
        if [ "$DRY_RUN" = true ]; then
          log_dry_run "Simulando arquivamento: $SRC → $DST"
          log_dry_run "Comando que seria executado: s3tar --region $REGION -vv -c -f $DST --concat-in-memory --storage-class DEEP_ARCHIVE --profile $PROFILE_NAME $SRC"
          echo "$PATH_KEY" >> "$PROCESSED_FILE"
          log_success "Caminho $PATH_KEY registrado como processado no arquivo $PROCESSED_FILE"
          ARCHIVE_SUCCESS=true
        else
          log_info "Arquivando $SRC → $DST"
          s3tar --region "$REGION" \
            -vv \
            -c \
            -f "$DST" \
            --concat-in-memory \
            --storage-class DEEP_ARCHIVE \
            --profile "$PROFILE_NAME" \
            "$SRC"
          
          ARCHIVE_SUCCESS=$?
          
          if [ "$ARCHIVE_SUCCESS" -eq 0 ]; then
            echo "$PATH_KEY" >> "$PROCESSED_FILE"
            log_success "Caminho $PATH_KEY registrado como processado"
          fi
        fi
        
        if [ "$ARCHIVE_SUCCESS" = true ] || [ "$ARCHIVE_SUCCESS" -eq 0 ]; then
          if [ "$SHOULD_DELETE" = true ]; then
            if [ "$DRY_RUN" = true ]; then
              log_dry_run "Simulando deleção de arquivos originais de $SRC"
              log_dry_run "Comando que seria executado: aws $PROFILE_ARG s3 rm $SRC --recursive"
            else
              log_info "Deletando arquivos originais de $SRC"
              aws $PROFILE_ARG s3 rm "$SRC" --recursive
            fi
          else
            log_success "Arquivado com sucesso, mas sem deleção"
          fi
          
          MONTH_PROCESSED=true
          YEAR_PROCESSED=true
        else
          log_error "Falha ao arquivar $SRC"
        fi
      done
      
      if [ "$MONTH_PROCESSED" = true ]; then
        echo "$COMPANY_SHORT:$OBJECT:$YEAR:$MONTH:ALL" >> "$PROCESSED_FILE"
        log_success "Mês $YEAR-$MONTH completo para $OBJECT/$COMPANY"
      fi
    done
    
    if [ "$YEAR_PROCESSED" = true ]; then
      echo "$COMPANY_SHORT:$OBJECT:$YEAR:ALL" >> "$PROCESSED_FILE"
      log_success "Ano $YEAR completo para $OBJECT/$COMPANY"
      
      LAST_YEAR=$(echo "$OBJECT_YEARS" | tail -n1 | awk -F= '{print $2}')
      if [ "$YEAR" = "$LAST_YEAR" ]; then
        echo "$COMPANY_SHORT:$OBJECT:ALL" >> "$PROCESSED_FILE"
        log_success "Objeto $OBJECT totalmente processado para empresa $COMPANY"
      fi
    fi
  done
done

