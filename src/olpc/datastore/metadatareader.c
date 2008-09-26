#include "Python.h"

#include <dirent.h>

// TODO: put it in a place where python can use it when writing metadata
#define MAX_PROPERTY_LENGTH 500 * 1024

static PyObject *byte_array_type = NULL;

static PyObject *
metadatareader_retrieve(PyObject *unused, PyObject *args)
{
    PyObject *dict = NULL;
    PyObject *properties = NULL;
    const char *dir_path = NULL;
    char *metadata_path = NULL;
	DIR *dir_stream = NULL;
	struct dirent *dir_entry = NULL;
    char *file_path = NULL;
    FILE *file = NULL;
	char *value_buf = NULL;

    if (!PyArg_ParseTuple(args, "sO:retrieve", &dir_path, &properties))
        return NULL;

    // Build path to the metadata directory
    int metadata_path_size = strlen(dir_path) + 10;
    metadata_path = PyMem_Malloc(metadata_path_size);
	if (metadata_path == NULL) {
        PyErr_NoMemory();
        goto cleanup;
	}
    snprintf (metadata_path, metadata_path_size, "%s/%s", dir_path, "metadata");

    dir_stream = opendir (metadata_path);
	if (dir_stream == NULL) {
	    char buf[256];
	    snprintf(buf, sizeof(buf), "Couldn't open metadata directory %s",
	             metadata_path);
        PyErr_SetString(PyExc_IOError, buf);
        goto cleanup;
	}

    dict = PyDict_New();

	dir_entry = readdir(dir_stream);
    while (dir_entry != NULL) {
        long file_size;
        int file_path_size;
        PyObject *value = NULL;

        // Skip . and ..
        if (dir_entry->d_name[0] == '.' &&
            (strlen(dir_entry->d_name) == 1 || 
                (dir_entry->d_name[1] == '.' &&
                    strlen(dir_entry->d_name) == 2)))
			goto next_property;

        // Check if the property is in the properties list
        if ((properties != Py_None) && (PyList_Size(properties) > 0)) {
            int found = 0;
            int i;
            for (i = 0; i < PyList_Size(properties); i++) {
                PyObject *property = PyList_GetItem(properties, i);
                if (!strcmp (dir_entry->d_name, PyString_AsString (property))) {
                    found = 1;
                }
            }
            if (!found) {
                goto next_property;
            }
        }

        // Build path of the property file
        file_path_size = strlen(metadata_path) + 1 + strlen(dir_entry->d_name) +
                         1;
        file_path = PyMem_Malloc(file_path_size);
    	if (file_path == NULL) {
            PyErr_NoMemory();
            goto cleanup;
    	}
        snprintf (file_path, file_path_size, "%s/%s", metadata_path,
                  dir_entry->d_name);

        file = fopen(file_path, "r");
        if (file == NULL) {
    	    char buf[256];
    	    snprintf(buf, sizeof(buf), "Cannot open property file %s: %s",
    	             file_path, strerror(errno));
            PyErr_SetString(PyExc_IOError, buf);
            goto cleanup;
        }

        // Get file size
        fseek (file, 0, SEEK_END);
        file_size = ftell (file);
        rewind (file);

        if (file_size == 0) {
            // Empty property
            value = PyString_FromString("");
            if (value == NULL) {
                PyErr_SetString(PyExc_ValueError,
                                "Failed to convert value to python string");
                goto cleanup;
            }
        } else {
            if (file_size > MAX_PROPERTY_LENGTH) {
                PyErr_SetString(PyExc_ValueError, "Property file too big");
                goto cleanup;
            }

            // Read the whole file
        	value_buf = PyMem_Malloc(file_size);
        	if (value_buf == NULL) {
                PyErr_NoMemory();
                goto cleanup;
        	}
            long read_size = fread(value_buf, 1, file_size, file);
            if (read_size < file_size) {
        	    char buf[256];
        	    snprintf(buf, sizeof(buf),
        	             "Error while reading property file %s", file_path);
                PyErr_SetString(PyExc_IOError, buf);
                goto cleanup;
            }

            // Convert value to dbus.ByteArray
            PyObject *args = Py_BuildValue("(s#)", value_buf, file_size);
            value = PyObject_CallObject(byte_array_type, args);
            if (value == NULL) {
                PyErr_SetString(PyExc_ValueError,
                                "Failed to convert value to dbus.ByteArray");
                goto cleanup;
            }
        }

        // Add property to the metadata dict
        if (PyDict_SetItemString(dict, dir_entry->d_name, value) == -1) {
            PyErr_SetString(PyExc_ValueError,
                            "Failed to add property to dictionary");
            goto cleanup;
        }

    next_property:
        if (file_path) {
            PyMem_Free(file_path);
            file_path = NULL;
        }
        if (file) {
            fclose(file);
            file = NULL;
        }
        if (value_buf) {
            PyMem_Free(value_buf);
            value_buf = NULL;
        }

    	dir_entry = readdir(dir_stream);
    }

	closedir(dir_stream);

    return dict;

cleanup:
    if (file_path) {
        PyMem_Free(file_path);
    }
    if (value_buf) {
        PyMem_Free(value_buf);
    }
    if (dict) {
        Py_DECREF(dict);
    }
    if (file) {
        fclose(file);
    }
    if (dir_stream) {
    	closedir(dir_stream);
	}
    return NULL;        
}

static PyMethodDef metadatareader_functions[] = {
    {"retrieve", metadatareader_retrieve, METH_VARARGS, PyDoc_STR("Read a dictionary from a file")},
    {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
initmetadatareader(void)
{
  PyObject* mod;
  mod = Py_InitModule("metadatareader", metadatareader_functions);
  
  PyObject *dbus_module = PyImport_ImportModule("dbus");
  byte_array_type = PyObject_GetAttrString(dbus_module, "ByteArray");
}

