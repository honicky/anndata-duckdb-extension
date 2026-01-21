#pragma once

#include <hdf5.h>
#include <string>
#include <stdexcept>
#include <vector>
#include <fstream>

namespace duckdb {

// Base class for HDF5 handles with common functionality
template <typename CloseFunc>
class H5Handle {
protected:
	hid_t id;
	CloseFunc close_func;

public:
	H5Handle(hid_t id, CloseFunc close_func) : id(id), close_func(close_func) {
	}

	~H5Handle() {
		if (id >= 0) {
			close_func(id);
		}
	}

	// Move constructor
	H5Handle(H5Handle &&other) noexcept : id(other.id), close_func(other.close_func) {
		other.id = -1;
	}

	// Move assignment
	H5Handle &operator=(H5Handle &&other) noexcept {
		if (this != &other) {
			if (id >= 0) {
				close_func(id);
			}
			id = other.id;
			close_func = other.close_func;
			other.id = -1;
		}
		return *this;
	}

	// Delete copy constructor and assignment
	H5Handle(const H5Handle &) = delete;
	H5Handle &operator=(const H5Handle &) = delete;

	operator hid_t() const {
		return id;
	}
	hid_t get() const {
		return id;
	}
	bool is_valid() const {
		return id >= 0;
	}

	// Release ownership without closing
	hid_t release() {
		hid_t temp = id;
		id = -1;
		return temp;
	}
};

// RAII wrapper for HDF5 file handle
class H5FileHandle {
private:
	hid_t id;

public:
	H5FileHandle() : id(-1) {
	}

	explicit H5FileHandle(const std::string &path, unsigned int flags = H5F_ACC_RDONLY) {
		// Try to open the file with default properties first
		id = H5Fopen(path.c_str(), flags, H5P_DEFAULT);

		if (id < 0) {
			// Check if file exists
			std::ifstream test_file(path);
			if (!test_file.good()) {
				throw std::runtime_error("File does not exist or cannot be read: " + path);
			}
			test_file.close();

			// File exists but HDF5 can't open it
			throw std::runtime_error("Failed to open HDF5 file (H5Fopen returned " + std::to_string(id) + "): " + path);
		}
	}

	// Constructor from existing handle (takes ownership)
	explicit H5FileHandle(hid_t file_id) : id(file_id) {
	}

	~H5FileHandle() {
		if (id >= 0) {
			H5Fclose(id);
		}
	}

	// Move constructor
	H5FileHandle(H5FileHandle &&other) noexcept : id(other.id) {
		other.id = -1;
	}

	// Move assignment
	H5FileHandle &operator=(H5FileHandle &&other) noexcept {
		if (this != &other) {
			if (id >= 0) {
				H5Fclose(id);
			}
			id = other.id;
			other.id = -1;
		}
		return *this;
	}

	// Delete copy constructor and assignment
	H5FileHandle(const H5FileHandle &) = delete;
	H5FileHandle &operator=(const H5FileHandle &) = delete;

	operator hid_t() const {
		return id;
	}
	hid_t get() const {
		return id;
	}
	bool is_valid() const {
		return id >= 0;
	}
};

// RAII wrapper for HDF5 group handle
class H5GroupHandle {
private:
	hid_t id;

public:
	H5GroupHandle() : id(-1) {
	}

	H5GroupHandle(hid_t loc_id, const std::string &name) {
		id = H5Gopen2(loc_id, name.c_str(), H5P_DEFAULT);
		if (id < 0) {
			throw std::runtime_error("Failed to open HDF5 group: " + name);
		}
	}

	// Constructor from existing handle (takes ownership)
	explicit H5GroupHandle(hid_t group_id) : id(group_id) {
	}

	~H5GroupHandle() {
		if (id >= 0) {
			H5Gclose(id);
		}
	}

	// Move constructor
	H5GroupHandle(H5GroupHandle &&other) noexcept : id(other.id) {
		other.id = -1;
	}

	// Move assignment
	H5GroupHandle &operator=(H5GroupHandle &&other) noexcept {
		if (this != &other) {
			if (id >= 0) {
				H5Gclose(id);
			}
			id = other.id;
			other.id = -1;
		}
		return *this;
	}

	// Delete copy constructor and assignment
	H5GroupHandle(const H5GroupHandle &) = delete;
	H5GroupHandle &operator=(const H5GroupHandle &) = delete;

	operator hid_t() const {
		return id;
	}
	hid_t get() const {
		return id;
	}
	bool is_valid() const {
		return id >= 0;
	}
};

// RAII wrapper for HDF5 dataset handle
class H5DatasetHandle {
private:
	hid_t id;

public:
	H5DatasetHandle() : id(-1) {
	}

	H5DatasetHandle(hid_t loc_id, const std::string &name) {
		id = H5Dopen2(loc_id, name.c_str(), H5P_DEFAULT);
		if (id < 0) {
			throw std::runtime_error("Failed to open HDF5 dataset: " + name);
		}
	}

	// Constructor from existing handle (takes ownership)
	explicit H5DatasetHandle(hid_t dataset_id) : id(dataset_id) {
	}

	~H5DatasetHandle() {
		if (id >= 0) {
			H5Dclose(id);
		}
	}

	// Move constructor
	H5DatasetHandle(H5DatasetHandle &&other) noexcept : id(other.id) {
		other.id = -1;
	}

	// Move assignment
	H5DatasetHandle &operator=(H5DatasetHandle &&other) noexcept {
		if (this != &other) {
			if (id >= 0) {
				H5Dclose(id);
			}
			id = other.id;
			other.id = -1;
		}
		return *this;
	}

	// Delete copy constructor and assignment
	H5DatasetHandle(const H5DatasetHandle &) = delete;
	H5DatasetHandle &operator=(const H5DatasetHandle &) = delete;

	operator hid_t() const {
		return id;
	}
	hid_t get() const {
		return id;
	}
	bool is_valid() const {
		return id >= 0;
	}
};

// RAII wrapper for HDF5 dataspace handle
class H5DataspaceHandle {
private:
	hid_t id;

public:
	H5DataspaceHandle() : id(-1) {
	}

	// Constructor that gets dataspace from dataset
	explicit H5DataspaceHandle(hid_t dataset_id) {
		id = H5Dget_space(dataset_id);
		if (id < 0) {
			throw std::runtime_error("Failed to get HDF5 dataspace");
		}
	}

	// Constructor for creating simple dataspace
	H5DataspaceHandle(int rank, const hsize_t *dims) {
		id = H5Screate_simple(rank, dims, nullptr);
		if (id < 0) {
			throw std::runtime_error("Failed to create HDF5 dataspace");
		}
	}

	// Constructor from existing handle (takes ownership)
	static H5DataspaceHandle from_handle(hid_t space_id) {
		H5DataspaceHandle handle;
		handle.id = space_id;
		return handle;
	}

	~H5DataspaceHandle() {
		if (id >= 0) {
			H5Sclose(id);
		}
	}

	// Move constructor
	H5DataspaceHandle(H5DataspaceHandle &&other) noexcept : id(other.id) {
		other.id = -1;
	}

	// Move assignment
	H5DataspaceHandle &operator=(H5DataspaceHandle &&other) noexcept {
		if (this != &other) {
			if (id >= 0) {
				H5Sclose(id);
			}
			id = other.id;
			other.id = -1;
		}
		return *this;
	}

	// Delete copy constructor and assignment
	H5DataspaceHandle(const H5DataspaceHandle &) = delete;
	H5DataspaceHandle &operator=(const H5DataspaceHandle &) = delete;

	operator hid_t() const {
		return id;
	}
	hid_t get() const {
		return id;
	}
	bool is_valid() const {
		return id >= 0;
	}
};

// RAII wrapper for HDF5 datatype handle
class H5DatatypeHandle {
private:
	hid_t id;
	bool should_close; // Some datatypes like H5T_NATIVE_* shouldn't be closed

public:
	H5DatatypeHandle() : id(-1), should_close(true) {
	}

	// Constructor that gets datatype from dataset
	explicit H5DatatypeHandle(hid_t dataset_id) : should_close(true) {
		id = H5Dget_type(dataset_id);
		if (id < 0) {
			throw std::runtime_error("Failed to get HDF5 datatype");
		}
	}

	// For predefined types that shouldn't be closed
	static H5DatatypeHandle from_native(hid_t native_type) {
		H5DatatypeHandle handle;
		handle.id = native_type;
		handle.should_close = false;
		return handle;
	}

	// Copy a datatype (for string types)
	static H5DatatypeHandle copy(hid_t type_id) {
		H5DatatypeHandle handle;
		handle.id = H5Tcopy(type_id);
		handle.should_close = true;
		if (handle.id < 0) {
			throw std::runtime_error("Failed to copy HDF5 datatype");
		}
		return handle;
	}

	~H5DatatypeHandle() {
		if (id >= 0 && should_close) {
			H5Tclose(id);
		}
	}

	// Move constructor
	H5DatatypeHandle(H5DatatypeHandle &&other) noexcept : id(other.id), should_close(other.should_close) {
		other.id = -1;
		other.should_close = false;
	}

	// Move assignment
	H5DatatypeHandle &operator=(H5DatatypeHandle &&other) noexcept {
		if (this != &other) {
			if (id >= 0 && should_close) {
				H5Tclose(id);
			}
			id = other.id;
			should_close = other.should_close;
			other.id = -1;
			other.should_close = false;
		}
		return *this;
	}

	// Delete copy constructor and assignment
	H5DatatypeHandle(const H5DatatypeHandle &) = delete;
	H5DatatypeHandle &operator=(const H5DatatypeHandle &) = delete;

	operator hid_t() const {
		return id;
	}
	hid_t get() const {
		return id;
	}
	bool is_valid() const {
		return id >= 0;
	}
};

// Alias for backward compatibility - H5TypeHandle is used in h5_reader_new.cpp
class H5TypeHandle {
private:
	hid_t id;
	bool should_close;

public:
	enum class TypeClass { DATASET, ATTRIBUTE };

	// Default constructor
	H5TypeHandle() : id(-1), should_close(true) {
	}

	// Constructor that gets datatype from dataset or attribute
	H5TypeHandle(hid_t obj_id, TypeClass type_class) : id(-1), should_close(true) {
		if (type_class == TypeClass::DATASET) {
			// Get type from dataset
			id = H5Dget_type(obj_id);
			if (id < 0) {
				throw std::runtime_error("Failed to get HDF5 datatype from dataset");
			}
		} else if (type_class == TypeClass::ATTRIBUTE) {
			// Get type from attribute
			id = H5Aget_type(obj_id);
			if (id < 0) {
				throw std::runtime_error("Failed to get HDF5 datatype from attribute");
			}
		}
	}

	~H5TypeHandle() {
		if (id >= 0 && should_close) {
			H5Tclose(id);
		}
	}

	// Move constructor
	H5TypeHandle(H5TypeHandle &&other) noexcept : id(other.id), should_close(other.should_close) {
		other.id = -1;
		other.should_close = false;
	}

	// Move assignment
	H5TypeHandle &operator=(H5TypeHandle &&other) noexcept {
		if (this != &other) {
			if (id >= 0 && should_close) {
				H5Tclose(id);
			}
			id = other.id;
			should_close = other.should_close;
			other.id = -1;
			other.should_close = false;
		}
		return *this;
	}

	// Delete copy constructor and assignment
	H5TypeHandle(const H5TypeHandle &) = delete;
	H5TypeHandle &operator=(const H5TypeHandle &) = delete;

	operator hid_t() const {
		return id;
	}
	hid_t get() const {
		return id;
	}
	bool is_valid() const {
		return id >= 0;
	}
};

// RAII wrapper for HDF5 attribute handle
class H5AttributeHandle {
private:
	hid_t id;

public:
	H5AttributeHandle() : id(-1) {
	}

	H5AttributeHandle(hid_t obj_id, const std::string &name) {
		id = H5Aopen(obj_id, name.c_str(), H5P_DEFAULT);
		if (id < 0) {
			throw std::runtime_error("Failed to open HDF5 attribute: " + name);
		}
	}

	// Constructor from existing handle (takes ownership)
	explicit H5AttributeHandle(hid_t attr_id) : id(attr_id) {
	}

	~H5AttributeHandle() {
		if (id >= 0) {
			H5Aclose(id);
		}
	}

	// Move constructor
	H5AttributeHandle(H5AttributeHandle &&other) noexcept : id(other.id) {
		other.id = -1;
	}

	// Move assignment
	H5AttributeHandle &operator=(H5AttributeHandle &&other) noexcept {
		if (this != &other) {
			if (id >= 0) {
				H5Aclose(id);
			}
			id = other.id;
			other.id = -1;
		}
		return *this;
	}

	// Delete copy constructor and assignment
	H5AttributeHandle(const H5AttributeHandle &) = delete;
	H5AttributeHandle &operator=(const H5AttributeHandle &) = delete;

	operator hid_t() const {
		return id;
	}
	hid_t get() const {
		return id;
	}
	bool is_valid() const {
		return id >= 0;
	}
};

// Helper macro for checking HDF5 return values
#define H5_CHECK(call)                                                                                                 \
	do {                                                                                                               \
		herr_t _h5_status = (call);                                                                                    \
		if (_h5_status < 0) {                                                                                          \
			throw std::runtime_error("HDF5 error in " #call);                                                          \
		}                                                                                                              \
	} while (0)

// Helper function to check if a link exists
inline bool H5LinkExists(hid_t loc_id, const std::string &name) {
	htri_t exists = H5Lexists(loc_id, name.c_str(), H5P_DEFAULT);
	return exists > 0;
}

// Helper function to check object type
inline H5O_type_t H5GetObjectType(hid_t loc_id, const std::string &name) {
	H5O_info_t info;
	memset(&info, 0, sizeof(info));
	herr_t status = H5Oget_info_by_name(loc_id, name.c_str(), &info, H5O_INFO_BASIC, H5P_DEFAULT);
	if (status < 0) {
		fprintf(stderr, "[HDF5 DEBUG] H5GetObjectType(%s): H5Oget_info_by_name failed with status %d\n",
		        name.c_str(), static_cast<int>(status));
		return H5O_TYPE_UNKNOWN;
	}
	fprintf(stderr, "[HDF5 DEBUG] H5GetObjectType(%s): info.type=%d (0=GROUP, 1=DATASET, 2=NAMED_TYPE)\n",
	        name.c_str(), static_cast<int>(info.type));
	return info.type;
}

} // namespace duckdb
