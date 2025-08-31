import re

class EventExtractor:
    def __init__(self):
        self.original_templates = {
            "E1": "[*]Adding an already existing block[*]",
            "E2": "[*]Verification succeeded for[*]",
            "E3": "[*]Served block[*]to[*]",
            "E4": "[*]Got exception while serving[*]to[*]",
            "E5": "[*]Receiving block[*]src:[*]dest:[*]",
            "E6": "[*]Received block[*]src:[*]dest:[*]of size[*]",
            "E7": "[*]writeBlock[*]received exception[*]",
            "E8": "[*]PacketResponder[*]for block[*]Interrupted[*]",
            "E9": "[*]Received block[*]of size[*]from[*]",
            "E10": "[*]PacketResponder[*]Exception[*]",
            "E11": "[*]PacketResponder[*]for block[*]terminating[*]",
            "E12": "[*]:Exception writing block[*]to mirror[*]",
            "E13": "[*]Receiving empty packet for block[*]",
            "E14": "[*]Exception in receiveBlock for block[*]",
            "E15": "[*]Changing block file offset of block[*]from[*]to[*]meta file offset to[*]",
            "E16": "[*]:Transmitted block[*]to[*]",
            "E17": "[*]:Failed to transfer[*]to[*]got[*]",
            "E18": "[*]Starting thread to transfer block[*]to[*]",
            "E19": "[*]Reopen Block[*]",
            "E20": "[*]Unexpected error trying to delete block[*]BlockInfo not found in volumeMap[*]",
            "E21": "[*]Deleting block[*]file[*]",
            "E22": "[*]BLOCK* NameSystem[*]allocateBlock:[*]",
            "E23": "[*]BLOCK* NameSystem[*]delete:[*]is added to invalidSet of[*]",
            "E24": "[*]BLOCK* Removing block[*]from neededReplications as it does not belong to any file[*]",
            "E25": "[*]BLOCK* ask[*]to replicate[*]to[*]",
            "E26": "[*]BLOCK* NameSystem[*]addStoredBlock: blockMap updated:[*]is added to[*]size[*]",
            "E27": "[*]BLOCK* NameSystem[*]addStoredBlock: Redundant addStoredBlock request received for[*]on[*]size[*]",
            "E28": "[*]BLOCK* NameSystem[*]addStoredBlock: addStoredBlock request received for[*]on[*]size[*]But it does not belong to any file[*]",
            "E29": "[*]PendingReplicationMonitor timed out block[*]",
        }
        
        # Convert [*] wildcards to regex patterns
        self.regex_templates = {}
        for eid, template in self.original_templates.items():
            # Escape special regex characters except [*]
            escaped = re.escape(template)
            # Convert escaped \[\*\] back to .*
            regex_pattern = escaped.replace(r'\[\*\]', '.*')
            self.regex_templates[eid] = regex_pattern
        
        # Compile regex patterns for efficiency
        self.compiled_templates = {eid: re.compile(pattern) for eid, pattern in self.regex_templates.items()}
        
        # Debug: Print the converted patterns (optional)
        print("Converted templates:")
        for eid, pattern in self.regex_templates.items():
            print(f"{eid}: {pattern}")
    
    def extract_event_id(self, log_line: str) -> str:
        """
        Given a raw log line, return the matching EventId.
        If no match is found, return 'UNKNOWN'.
        """
        for eid, regex in self.compiled_templates.items():
            if regex.search(log_line):
                return eid
        return "UNKNOWN"
    
    def test_extraction(self, sample_logs):
        """Helper method to test extraction on sample logs"""
        print("\nTesting event extraction:")
        print("-" * 50)
        
        for i, log_line in enumerate(sample_logs):
            event_id = self.extract_event_id(log_line)
            print(f"Log {i+1}: {event_id}")
            print(f"  Text: {log_line}")
            
            # Show which pattern matched (if any)
            if event_id != "UNKNOWN":
                print(f"  Matched: {self.original_templates[event_id]}")
            print()

# Test the extractor
if __name__ == "__main__":
    extractor = EventExtractor()
    
    # sample logs
    sample_logs = [
        "2024-01-01 10:00:00 INFO Adding an already existing block blk_123",
        "2024-01-01 10:00:01 INFO Verification succeeded for blk_456", 
        "2024-01-01 10:00:02 INFO Served block blk_789 to /192.168.1.1:50010",
        "2024-01-01 10:00:03 ERROR Got exception while serving blk_101 to client",
        "2024-01-01 10:00:04 DEBUG Some unmatched log message"
    ]
    
    extractor.test_extraction(sample_logs)