#!/bin/bash

# --- CONFIG ---
XGB_LIB_PATH="$HOME/mambaforge/envs/spark_env/lib/python3.10/site-packages/xgboost/lib"
LIBOMP_PATH="$HOME/local/lib/libomp.dylib"
LIBXGBOOST="$XGB_LIB_PATH/libxgboost.dylib"

echo "🔧 Patching libxgboost.dylib to hardcode libomp.dylib..."

# Check existence
if [[ ! -f "$LIBOMP_PATH" ]]; then
  echo "❌ ERROR: libomp.dylib not found at $LIBOMP_PATH"
  exit 1
fi

if [[ ! -f "$LIBXGBOOST" ]]; then
  echo "❌ ERROR: libxgboost.dylib not found at $LIBXGBOOST"
  exit 1
fi

# Optional backup
cp "$LIBXGBOOST" "$LIBXGBOOST.bak"

# Patch the RPATH dependency to point directly to your libomp.dylib
install_name_tool -change @rpath/libomp.dylib "$LIBOMP_PATH" "$LIBXGBOOST"

echo "✅ Patch complete."
echo "💡 Restart Jupyter Notebook and try importing SparkXGBRegressor again."
