import io
import fsspec
import enum
import ujson

try:
    import tifffile
except ModuleNotFoundError:  # pragma: no cover
    raise ImportError(
        "tifffile is required for kerchunking TIFF files. Please install with "
        "`pip/conda install tifffile`."
    )

import kerchunk.utils


def tiff_to_zarr(urlpath, remote_options=None, target=None, target_options=None):
    """Wraps TIFFFile's fsspec writer to extract metadata as attributes

    Parameters
    ----------
    urlpath: str
        Location of input TIFF
    remote_options: dict
        pass these to fsspec when opening urlpath
    target: str
        Write JSON to this location. If not given, no file is output
    target_options: dict
        pass these to fsspec when opening target

    Returns
    -------
    references dict
    """

    with fsspec.open(urlpath, **(remote_options or {})) as of:
        url, name = urlpath.rsplit("/", 1)

        with tifffile.TiffFile(of, name=name) as tif:
            with tif.series[0].aszarr() as store:
                of2 = io.StringIO()
                store.write_fsspec(of2, url=url)
                out = ujson.loads(of2.getvalue())

                meta = ujson.loads(out[".zattrs"])
                for k in dir(tif):
                    if not k.endswith("metadata"):
                        continue
                    try:
                        met = getattr(tif, k, None)
                    except Exception:
                        continue
                    try:
                        d = dict(met or {})
                    except ValueError:
                        # newer tifffile exposes xml structured tags
                        from xml.etree import ElementTree

                        e = ElementTree.fromstring(met)
                        d = {i.get("name"): i.text for i in e}
                    meta.update(d)
                for k, v in meta.copy().items():
                    # deref enums
                    if isinstance(v, enum.EnumMeta):
                        meta[k] = v._name_
                out[".zattrs"] = ujson.dumps(meta)
    # If tiff is zarr array, convert into a zarr group for Xarray IO
    # if out == array, convert to group, assign additional attrs
    out = {"data/" + k: v for k, v in out.items()}
    out[".zgroup"] = '{"zarr_format": 2}'

    if "GTRasterTypeGeoKey" in meta:
        try:
            import rioxarray
            import rasterio
        except ModuleNotFoundError:  # pragma: no cover
            raise ImportWarning(
        "rioxarray/rasterio is required for generating latitude, longitude values.")
        import zarr
        fs = fsspec.filesystem("reference", fo=out)
        z = zarr.open(fs.get_mapper())
        # coords = generate_coords(meta, z[0].shape)
        crs = rasterio.crs.CRS.from_epsg(meta['GeographicTypeGeoKey'])
        print(urlpath)
        rds = rioxarray.open_rasterio(urlpath)
        projected = rds.rio.reproject(crs)
        lon = projected.x.values.tolist()
        lat = projected.y.values.tolist()
        out["lat/0.0"] = lat
        out["lat/.zattrs"] = ""
        out["lat/.zarray"] = ""
        out["lon/0.0"] = lon
        out["lon/.zattrs"] = ""
        out["lon/.zarray"] = ""
        # To Do: Assign lat and lon cord arrays to reference file. How to open those and how does xarray open those
    if target is not None:
        with fsspec.open(target, **(target_options or {})) as of:
            ujson.dump(out, of)
    return out


# http://geotiff.maptools.org/spec/geotiff6.html#6.3.1.3
units = {
    9001: "metre",
    9002: "foot",
    9003: "US survey foot",
    9015: "mile international nautical",  # ... and many more
}


TiffToZarr = kerchunk.utils.class_factory(tiff_to_zarr)


def generate_coords(attrs, shape):
    """Produce coordinate arrays for given variable

    Specific to GeoTIFF input attributes

    Parameters
    ----------
    attrs: dict
        Containing the geoTIFF tags, probably the root group of the dataset
    shape: tuple[int]
        The array size in numpy (C) order
    """
    import numpy as np

    height, width = shape[-2:]
    xscale, yscale, zscale = attrs["ModelPixelScale"][:3]
    x0, y0, z0 = attrs["ModelTiepoint"][3:6]
    out = {}
    out["x"] = np.arange(width) * xscale + x0 + xscale / 2
    out["y"] = np.arange(height) * -yscale + y0 - yscale / 2
    if len(shape) > 2:
        out["z"] = np.arange(shape[-3]) * zscale + z0 + zscale / 2
    return out
