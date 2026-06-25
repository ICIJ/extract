#!/usr/bin/env python3
"""libpff attachment-recovery bridge for ResilientOutlookPSTParser.

Reads a single by-value attachment that java-libpst could not read from an OST/PST
file and writes its raw bytes to stdout. Selection is by the message descriptor node
id (== java-libpst getDescriptorNodeId() == pypff message.identifier) plus the
attachment index, so it recovers exactly the attachment the JVM side failed on.

Usage: pff_read_attachment.py <pst_path> <message_identifier> <attachment_index>
Exit:  0 + bytes on stdout on success; non-zero with a message on stderr otherwise.
"""
import sys

def main():
    if len(sys.argv) != 4:
        sys.stderr.write("usage: pff_read_attachment.py <pst> <msg_id> <att_idx>\n"); return 2
    path, msg_id, att_idx = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    try:
        import pypff
    except ImportError:
        sys.stderr.write("pypff not available\n"); return 3
    f = pypff.file()
    f.open(path)
    try:
        att = find_attachment(f.get_root_folder(), msg_id, att_idx)
        if att is None:
            sys.stderr.write("message/attachment not found\n"); return 4
        size = att.get_size()
        out = sys.stdout.buffer
        # read_buffer caps at the libbfio block size; loop to drain the whole attachment.
        remaining = size
        while remaining > 0:
            chunk = att.read_buffer(min(remaining, 1 << 20))
            if not chunk:
                break
            out.write(chunk); remaining -= len(chunk)
        out.flush()
        return 0
    finally:
        f.close()

def find_attachment(folder, msg_id, att_idx):
    # Isolate every descriptor read: a folder/message libpff cannot enumerate (this OST has some)
    # must not abort the search for an attachment that lives elsewhere in the tree.
    try:
        n_sub = folder.number_of_sub_folders
    except Exception:
        n_sub = 0
    for i in range(n_sub):
        try:
            sub = folder.get_sub_folder(i)
        except Exception:
            continue
        hit = find_attachment(sub, msg_id, att_idx)
        if hit is not None:
            return hit
    try:
        n_msg = folder.number_of_sub_messages
    except Exception:
        n_msg = 0
    for i in range(n_msg):
        try:
            m = folder.get_sub_message(i)
            if m.identifier == msg_id and 0 <= att_idx < m.number_of_attachments:
                return m.get_attachment(att_idx)
        except Exception:
            continue
    return None

if __name__ == "__main__":
    sys.exit(main())
