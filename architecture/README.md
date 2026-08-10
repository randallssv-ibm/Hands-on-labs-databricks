### 3️⃣ `/architecture/README.md`

```markdown
# Architecture

Reference design and base scripts for structuring solutions in Databricks.


## 🏗️ Medallion Pattern

Implementation of the data architecture recommended by Databricks:
Bronze Layer → Silver Layer → Gold Layer

(Raw Data) (Cleaned) (Analytics)
## 📋 Contents

- **medallion-pattern.md** - Detailed explanation
- **catalog-definition.yaml** - Catalog definition
- **scripts/setup.sh** - Initialization script

## 🔧 How to use

```bash
# 1. Review the pattern
cat medallion-pattern.md

# 2. Run setup
bash scripts/setup.sh

# 3. Validate the catalog
python scripts/validate-catalog.py
