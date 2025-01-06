import time
import logging
import traceback
import importlib
from fastapi.responses import (
    JSONResponse,
)
from fastapi import (
    FastAPI,
    APIRouter,
    Request,
    status,
)
from starlette.middleware.base import (
    BaseHTTPMiddleware,
    RequestResponseEndpoint
)
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from extension import (
    mongo
)
from config import Config


app = FastAPI(
    title='app',
    openapi_url='/api/v1/openapi.json',
    redoc_url='/api/v1/redoc',
    description="my fastapi service",
)
logger = logging.getLogger('app')


# only support 'http'
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


class ExceptionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> JSONResponse:
        try:
            response = await call_next(request)
        except Exception as e:
            logger.exception(f"got exception {e}, stack: {traceback.format_exc()}")
            response = JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"message": "internal error", "status": "bad"},
            )
        return response


origins = ["*"]
app.add_middleware(ExceptionMiddleware)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


def configure_database():
    conf = Config.model_dump()
    logger.debug(f"Config mode={Config.MODE}")
    mongo.load_config(conf['MONGO_MAP'])


def configure_routers():
    router_path = 'asn'
    router = 'router'
    try:
        pkg = importlib.import_module(router_path)
        router_instance = getattr(pkg, router)
        assert isinstance(router_instance, APIRouter)
        logger.info(f'loading endpoint: {router_path}:{router}')
        app.include_router(router_instance, prefix='/api/v1')
    except Exception as e:
        logger.exception(e)
        raise e
    

def config_app():
    configure_routers()
    configure_database()


config_app()
