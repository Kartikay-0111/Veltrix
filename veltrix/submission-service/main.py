# submission-service/main.py
import uuid, os, boto3, redis, asyncpg, asyncio
from botocore.exceptions import ClientError
from fastapi import FastAPI, File, UploadFile, Header, HTTPException
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_host: str
    postgres_port: int = 5432
    redis_host: str
    redis_port: int = 6379
    minio_host: str
    minio_port: int = 9000
    minio_root_user: str
    minio_root_password: str
    minio_bucket: str

cfg = Settings()
app = FastAPI(title="IICPC Submission Service")

# ── Clients (initialised on startup) ─────────────────────────────
db_pool: asyncpg.Pool = None
redis_client: redis.Redis = None
s3: boto3.client = None
MULTIPART_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB (>= S3 5MB minimum part size)

@app.on_event("startup")
async def startup():
    global db_pool, redis_client, s3

    db_pool = await asyncpg.create_pool(
        user=cfg.postgres_user, password=cfg.postgres_password,
        database=cfg.postgres_db, host=cfg.postgres_host, port=cfg.postgres_port
    )

    redis_client = redis.Redis(
        host=cfg.redis_host, port=cfg.redis_port, decode_responses=True
    )

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{cfg.minio_host}:{cfg.minio_port}",
        aws_access_key_id=cfg.minio_root_user,
        aws_secret_access_key=cfg.minio_root_password,
    )

    # Create bucket if it doesn't exist
    try:
        s3.create_bucket(Bucket=cfg.minio_bucket)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise

# ── Auth helper ───────────────────────────────────────────────────
async def get_team(api_key: str):
    team = await db_pool.fetchrow(
        "SELECT * FROM teams WHERE api_key = $1", api_key
    )
    if not team:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return team

async def upload_stream_to_s3(upload: UploadFile, bucket: str, key: str) -> None:
    """Stream upload to S3/MinIO without loading the whole file into memory."""
    upload_id = None
    parts = []
    try:
        resp = await asyncio.to_thread(
            s3.create_multipart_upload, Bucket=bucket, Key=key
        )
        upload_id = resp["UploadId"]
        part_number = 1

        while True:
            chunk = await upload.read(MULTIPART_CHUNK_SIZE)
            if not chunk:
                break
            part_resp = await asyncio.to_thread(
                s3.upload_part,
                Bucket=bucket,
                Key=key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=chunk,
            )
            parts.append({"PartNumber": part_number, "ETag": part_resp["ETag"]})
            part_number += 1

        if not parts:
            await asyncio.to_thread(
                s3.abort_multipart_upload,
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
            )
            await asyncio.to_thread(
                s3.put_object, Bucket=bucket, Key=key, Body=b""
            )
        else:
            await asyncio.to_thread(
                s3.complete_multipart_upload,
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
    except Exception:
        if upload_id:
            await asyncio.to_thread(
                s3.abort_multipart_upload,
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
            )
        raise

# ── Routes ────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/submit")
async def submit(
    file: UploadFile = File(...),
    language: str = "cpp",
    x_api_key: str = Header(...)
):
    """
    Contestants POST their binary/source here.
    Returns a submission_id they can poll for status.
    """
    team = await get_team(x_api_key)

    # Validate language
    if language not in ("cpp", "rust", "go"):
        raise HTTPException(400, "Language must be cpp, rust, or go")

    submission_id = str(uuid.uuid4())
    storage_key = f"{team['id']}/{submission_id}/{file.filename}"

    # 1. Stream upload to MinIO
    await upload_stream_to_s3(file, cfg.minio_bucket, storage_key)

    # 2. Write submission record to Postgres
    await db_pool.execute("""
        INSERT INTO submissions (id, team_id, language, status, storage_key)
        VALUES ($1, $2, $3, 'PENDING', $4)
    """, submission_id, str(team['id']), language, storage_key)

    # 3. Push job to Redis queue (sandbox manager listens here)
    await asyncio.to_thread(redis_client.rpush, "submission_queue", submission_id)

    return {
        "submission_id": submission_id,
        "status": "PENDING",
        "message": "Submission received. Container will be ready shortly."
    }

@app.get("/submission/{submission_id}")
async def get_submission(submission_id: str, x_api_key: str = Header(...)):
    """Poll this to check if your sandbox is ready."""
    team = await get_team(x_api_key)

    row = await db_pool.fetchrow(
        "SELECT * FROM submissions WHERE id = $1 AND team_id = $2",
        submission_id, str(team["id"])
    )
    if not row:
        raise HTTPException(404, "Submission not found")

    return {
        "submission_id": submission_id,
        "status": row["status"],
        "endpoint_url": row["endpoint_url"],
        "error": row["error_message"]
    }