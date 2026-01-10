#!/usr/bin/env python3
"""
Simple HTTP server with Range request support for testing remote HDF5 access.
"""

import http.server
import socketserver
import os
import sys
import argparse


class RangeHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP handler that supports Range requests (HTTP 206 Partial Content)."""

    def do_HEAD(self):
        """Handle HEAD requests - send headers without body."""
        path = self.translate_path(self.path)
        if not os.path.isfile(path):
            self.send_error(404, "File not found")
            return

        file_size = os.path.getsize(path)

        # Check for Range header (shouldn't normally be in HEAD, but handle it)
        range_header = self.headers.get("Range")
        if range_header:
            try:
                range_spec = range_header.replace("bytes=", "")
                parts = range_spec.split("-")
                start = int(parts[0]) if parts[0] else 0
                end = int(parts[1]) if parts[1] else file_size - 1
                end = min(end, file_size - 1)

                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
                self.send_header("Content-Length", end - start + 1)
            except (ValueError, IndexError):
                self.send_response(200)
                self.send_header("Content-Length", file_size)
        else:
            self.send_response(200)
            self.send_header("Content-Length", file_size)

        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Type", self.guess_type(path))
        self.end_headers()

    def send_head(self):
        path = self.translate_path(self.path)
        if not os.path.isfile(path):
            self.send_error(404, "File not found")
            return None

        file_size = os.path.getsize(path)

        # Check for Range header
        range_header = self.headers.get("Range")
        if range_header:
            try:
                # Parse Range: bytes=start-end
                range_spec = range_header.replace("bytes=", "")
                parts = range_spec.split("-")
                start = int(parts[0]) if parts[0] else 0
                end = int(parts[1]) if parts[1] else file_size - 1

                # Clamp to file size
                end = min(end, file_size - 1)

                self.send_response(206)  # Partial Content
                self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
                self.send_header("Content-Length", end - start + 1)
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Content-Type", self.guess_type(path))
                self.end_headers()

                f = open(path, "rb")
                f.seek(start)
                return (f, start, end - start + 1)
            except (ValueError, IndexError) as e:
                self.send_error(416, f"Invalid Range: {e}")
                return None
        else:
            # No Range header - send full file
            self.send_response(200)
            self.send_header("Content-Length", file_size)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Type", self.guess_type(path))
            self.end_headers()
            return open(path, "rb")

    def copyfile(self, source, outputfile):
        if isinstance(source, tuple):
            f, start, length = source
            outputfile.write(f.read(length))
            f.close()
        else:
            # Full file
            super().copyfile(source, outputfile)

    def guess_type(self, path):
        if path.endswith(".h5ad") or path.endswith(".hdf5") or path.endswith(".h5"):
            return "application/x-hdf5"
        return super().guess_type(path)


def main():
    parser = argparse.ArgumentParser(description="HTTP server with Range request support")
    parser.add_argument("-p", "--port", type=int, default=8080, help="Port to listen on")
    parser.add_argument("-d", "--directory", default=".", help="Directory to serve files from")
    args = parser.parse_args()

    os.chdir(args.directory)

    handler = RangeHTTPRequestHandler

    with socketserver.TCPServer(("", args.port), handler) as httpd:
        print(f"Serving at http://localhost:{args.port}")
        print(f"Directory: {os.getcwd()}")
        print("Press Ctrl+C to stop")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down...")


if __name__ == "__main__":
    main()
