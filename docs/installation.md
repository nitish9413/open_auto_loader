# 📦 Installation

OpenAutoLoader is designed to be lightweight and highly compatible. It requires **Python 3.10** or higher.

---

## 🐍 Standard Installation
The easiest way to get started is by installing the core package via `pip`:

```bash
pip install open-auto-loader
```

## ⚡ Using uv (Recommended)
For modern Python projects using [uv](https://github.com/astral-sh/uv), you can add it to your project instantly:

```bash
uv add open-auto-loader
```

---

## ☁️ Cloud Storage Dependencies
OpenAutoLoader uses `fsspec` and specialized drivers to communicate with cloud providers. Depending on where your data lives, you will need to install the corresponding backend:

| Cloud Provider | Storage Type | Required Driver |
| :--- | :--- | :--- |
| **AWS** | S3 | `s3fs` |
| **Azure** | Blob / Gen2 | `adlfs` |
| **Google Cloud** | GCS | `gcsfs` |

### Install Cloud Drivers
```bash
# For AWS S3
pip install s3fs

# For Azure Blob Storage
pip install adlfs

# For Google Cloud Storage
pip install gcsfs
```

---

## 🛠️ Verification
To verify that the installation was successful and all dependencies are linked, run the following in your terminal:

```bash
python -c "import open_auto_loader; print('OpenAutoLoader installed successfully!')"
```

---

## 🧪 Development Version
If you want to test the latest features directly from the source code, you can install the development version:

```bash
git clone https://github.com/nitish9413/open_auto_loader.git
cd open_auto_loader
pip install -e .
```
