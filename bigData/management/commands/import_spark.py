# bigData/management/commands/import_spark.py
from __future__ import annotations
from pathlib import Path
import tempfile
import zipfile
from typing import List, Optional, Tuple

from django.core.management.base import BaseCommand
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, get_json_object, to_date, lpad, regexp_replace, when, lit, to_json, struct
)
from pyspark.sql.types import ArrayType, MapType, StructType

from pyspark.sql.functions import current_timestamp

# === Config padrão (sem .env) ===
BASE_DIR = Path(__file__).resolve().parents[3]  # raiz do projeto (onde fica manage.py)
JDBC_JAR = BASE_DIR / "jars" / "postgresql-42.7.3.jar"  # garanta esse JAR no repo
DEFAULT_INPUT_PATH = (BASE_DIR / "data" / "cnpjs_sample.zip").resolve() #zip de teste

DEFAULT_JDBC_URL = "jdbc:postgresql://localhost:5432/opencnpj?sslmode=disable"
DEFAULT_USER     = "appuser"
DEFAULT_PASS     = "12345"
DEFAULT_SCHEMA   = "public"
DEFAULT_RAW_TBL  = "opencnpj_raw_json"   # será (re)criada se usar --mode overwrite
DEFAULT_OUT_TBL  = "empresas_curated"    # será (re)criada com o modo escolhido


# ----------------- helpers de Spark/JSON -----------------
def make_spark(app_name: str) -> SparkSession:
    if not JDBC_JAR.exists():
        raise FileNotFoundError(
            f"Driver JDBC não encontrado em {JDBC_JAR}.\n"
            f"Dica: mkdir -p jars && curl -L -o {JDBC_JAR} "
            f"https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"
        )
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars", str(JDBC_JAR))
        .config("spark.driver.extraClassPath", str(JDBC_JAR))
        .config("spark.executor.extraClassPath", str(JDBC_JAR))
        .config("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))
        .getOrCreate()
    )


def read_json_like(spark: SparkSession, path: str, multiline: bool) -> DataFrame:
    return spark.read.option("multiLine", multiline).json(path)


def to_raw_json_col(df: DataFrame) -> DataFrame:
    # 1 coluna 'raw_json' (string) com o objeto inteiro
    return df.select(to_json(struct(*df.columns)).alias("raw_json"))


def coerce_complex_to_json_str(df: DataFrame) -> DataFrame:
    # Converte arrays/maps/structs para JSON string, mantendo primitivas
    cols = []
    for f in df.schema.fields:
        c = col(f.name)
        dt = f.dataType
        if isinstance(dt, (ArrayType, MapType, StructType)):
            cols.append(to_json(c).alias(f.name))
        else:
            cols.append(c)
    return df.select(*cols)


def read_zip_streaming_json(
    spark: SparkSession,
    zip_path: Path,
    multiline: bool,
    temp_dir: Optional[Path],
    force_raw_before_union: bool,
    coerce_complex_before_union: bool,
    verbose: bool = True,
) -> Tuple[DataFrame, List[Path]]:
    """Extrai .json/.json.gz de um ZIP para arquivos temporários,
    lê cada um, normaliza e faz unionByName. NÃO apaga os temps aqui."""
    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP não encontrado: {zip_path}")

    temp_base = Path(temp_dir) if temp_dir else Path(tempfile.gettempdir())
    combined: Optional[DataFrame] = None
    processed, errors = 0, 0
    temp_paths: List[Path] = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        entries = [info for info in zf.infolist()
                   if (not info.is_dir()) and (info.filename.lower().endswith(".json") or info.filename.lower().endswith(".json.gz"))]
        if not entries:
            raise RuntimeError(f"Nenhum .json dentro de {zip_path}")

        total = len(entries)
        for idx, info in enumerate(entries, start=1):
            suffix = ".json.gz" if info.filename.lower().endswith(".json.gz") else ".json"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir=temp_base) as tmp:
                tmp_path = Path(tmp.name)
            temp_paths.append(tmp_path)

            if verbose:
                print(f"[{idx}/{total}] Extraindo {info.filename} → {tmp_path.name}")

            try:
                with zf.open(info, "r") as src, open(tmp_path, "wb") as dst:
                    for chunk in iter(lambda: src.read(1024 * 1024), b""):
                        dst.write(chunk)

                df = read_json_like(spark, str(tmp_path), multiline=multiline)

                if force_raw_before_union:
                    df = to_raw_json_col(df)
                elif coerce_complex_before_union:
                    df = coerce_complex_to_json_str(df)

                if verbose:
                    try:
                        df.limit(1).show(truncate=False)
                    except Exception as e:
                        print(f"  ! Preview falhou: {e}")

                combined = df if combined is None else combined.unionByName(df, allowMissingColumns=True)
                processed += 1
            except Exception as e:
                errors += 1
                print(f"  ! Erro no arquivo {info.filename}: {e}")

    if combined is None or processed == 0:
        raise RuntimeError(
            f"Nenhum JSON válido processado em {zip_path} "
            f"(processados={processed}, erros={errors}). "
            f"Avalie usar --multiline."
        )
    if verbose:
        print(f"Arquivos processados com sucesso: {processed} | erros: {errors}")

    return combined, temp_paths
# ---------------------------------------------------------


class import_spark:
   

    def __init__(
        self,
        jdbc_url: str = DEFAULT_JDBC_URL,
        db_user: str = DEFAULT_USER,
        db_pass: str = DEFAULT_PASS,
        schema: str = DEFAULT_SCHEMA,
        raw_table: str = DEFAULT_RAW_TBL,
        out_table: str = DEFAULT_OUT_TBL,
        mode: str = "overwrite",     # 'overwrite' ou 'append' na escrita das tabelas
        multiline: bool = False,    
        limit: Optional[int] = None, # limite de linhas na ingestão (debug)
        input_path: Optional[str] = str(DEFAULT_INPUT_PATH),
        tmpdir: Optional[str] = None,
    ):
        self.jdbc_url = jdbc_url
        self.db_user = db_user
        self.db_pass = db_pass
        self.schema = schema
        self.raw_table = raw_table
        self.out_table = out_table
        self.mode = mode
        self.multiline = multiline
        self.limit = limit
        self.input_path = input_path
        self.tmpdir = Path(tmpdir) if tmpdir else None

        self.spark = make_spark("import_spark")
        self.props = {"user": self.db_user, "password": self.db_pass, "driver": "org.postgresql.Driver"}

    def ingest_if_needed(self):
        if not self.input_path or str(self.input_path).strip() in {"-", "", "none", "None"}:
            print("Ingestão RAW pulada (nenhum --input real informado). Usando tabela RAW existente.")
            return

        path = Path(self.input_path)

        if path.suffix.lower() == ".zip":
            df_raw, temp_paths = read_zip_streaming_json(
                self.spark,
                zip_path=path,
                multiline=self.multiline,
                temp_dir=self.tmpdir,
                force_raw_before_union=True,       # força 1 col 'raw_json' por arquivo (tabelas compatíveis)
                coerce_complex_before_union=False,
            )
            if self.limit:
                df_raw = df_raw.limit(self.limit)

            # Mostra e grava RAW
            print("Schema RAW (esperado: ['raw_json']):")
            df_raw.printSchema()
            df_raw.show(10, truncate=False)

            (df_raw.write.format("jdbc")
                .option("url", self.jdbc_url)
                .option("dbtable", f"{self.schema}.{self.raw_table}")
                .option("user", self.db_user)
                .option("password", self.db_pass)
                .option("driver", "org.postgresql.Driver")
                .mode(self.mode)
                .save())

            #remover os temporários
            for p in temp_paths:
                try:
                    p.unlink(missing_ok=True)
                except Exception as e:
                    print(f"Aviso: não consegui remover temp {p}: {e}")

        else:
            # leitura direta (diretório/padrão .json/.json.gz)
            df_raw = read_json_like(self.spark, str(path), multiline=self.multiline)
            df_raw = to_raw_json_col(df_raw)
            df_raw = df_raw.withColumn("insert_dt", current_timestamp())
            if self.limit:
                df_raw = df_raw.limit(self.limit)

            print("Schema RAW (direto):")
            df_raw.printSchema()
            df_raw.show(10, truncate=False)

            (df_raw.write.format("jdbc")
                .option("url", self.jdbc_url)
                .option("dbtable", f"{self.schema}.{self.raw_table}")
                .option("user", self.db_user)
                .option("password", self.db_pass)
                .option("driver", "org.postgresql.Driver")
                .mode(self.mode)
                .save())

        print(f"✅ RAW criada/preenchida: {self.schema}.{self.raw_table} (mode={self.mode})")

    # ---------------- CURATE ----------------
    def curate(self):
        df_src = self.spark.read.jdbc(self.jdbc_url, f"{self.schema}.{self.raw_table}", properties=self.props)
        ing_col = col("insert_dtm") if "ingested_at" in df_src.columns else current_timestamp()

        def j(path: str):
            return get_json_object(col("raw_json"), path)

        df = df_src.select(
            lpad(
                when((j('$.cnpj').isNull()) | (j('$.cnpj') == ''), lit(None)).otherwise(j('$.cnpj')),
                14, '0'
            ).alias("cnpj"),
            j('$.situacao_cadastral').alias("situacao_cadastral"),
            to_date(j('$.data_situacao_cadastral'), "yyyy-MM-dd").alias("data_situacao_cadastral"),
            to_date(j('$.data_inicio_atividade'),  "yyyy-MM-dd").alias("data_inicio_atividade"),
            j('$.natureza_juridica').alias("natureza_juridica"),
            j('$.uf').alias("uf"),
            j('$.municipio').alias("municipio"),
            regexp_replace(regexp_replace(j('$.capital_social'), r'\.', ''), ',', '.')
                .cast("decimal(18,2)").alias("capital_social"),
            j('$.porte_empresa').alias("porte_empresa"),
            ing_col.alias("insert_dtm"),
        )

        print("Schema curated:")
        df.printSchema()
        df.show(20, truncate=False)

        (df.write.format("jdbc")
           .option("url", self.jdbc_url)
           .option("dbtable", f"{self.schema}.{self.out_table}")
           .option("user", self.db_user)
           .option("password", self.db_pass)
           .option("driver", "org.postgresql.Driver")
           .mode(self.mode)
           .save())

        print(f"✅ CURATED escrita: {self.schema}.{self.out_table} (mode={self.mode})")

    # ---------------- RUN ----------------
    def run(self):
        try:
            self.ingest_if_needed()
            self.curate()
        finally:
            self.spark.stop()


class Command(BaseCommand):
    help = "Importa JSONs (opcional) para RAW e cria CURATED usando Spark (sem .env)."

    def add_arguments(self, parser):
        # Conexão
        parser.add_argument("--jdbc-url", default=DEFAULT_JDBC_URL)
        parser.add_argument("--db-user", default=DEFAULT_USER)
        parser.add_argument("--db-pass", default=DEFAULT_PASS)
        parser.add_argument("--schema", default=DEFAULT_SCHEMA)

        # Tabelas
        parser.add_argument("--raw-table", default=DEFAULT_RAW_TBL)
        parser.add_argument("--out-table", default=DEFAULT_OUT_TBL)

        # Modo de escrita
        parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])

        # Ingestão opcional
        parser.add_argument( "--input",default=str(DEFAULT_INPUT_PATH))
        parser.add_argument("--multiline", action="store_true", help="Use se os JSONs não forem NDJSON")
        parser.add_argument("--limit", type=int, help="Limitar linhas na ingestão (debug)")
        parser.add_argument("--tmpdir", help="Diretório p/ temporários ao ler ZIP")

    def handle(self, *args, **opts):
        job = import_spark(
            jdbc_url=opts["jdbc_url"],
            db_user=opts["db_user"],
            db_pass=opts["db_pass"],
            schema=opts["schema"],
            raw_table=opts["raw_table"],
            out_table=opts["out_table"],
            mode=opts["mode"],
            multiline=opts["multiline"],
            limit=opts["limit"],
            input_path=opts.get("input"),
            tmpdir=opts.get("tmpdir"),
        )
        job.run()
