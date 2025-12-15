import requests
import logging
import time
from typing import List, Dict, Any, Optional
import json

# Configure logging
logger = logging.getLogger('outlook-email.sarvam')

class SarvamClient:
    def __init__(self, api_key: str, base_url: str = "https://api.sarvam.ai"):
        """
        Initialize the Sarvam AI client.
        
        Args:
            api_key (str): Sarvam API subscription key
            base_url (str): Base URL for Sarvam API
        """
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "API-Subscription-Key": api_key,
            "Content-Type": "application/json"
        }
    
    def generate_embeddings(self, texts: List[str], max_retries: int = 3) -> Optional[List[List[float]]]:
        """
        Generate embeddings for a list of texts using Sarvam API.
        
        Note: Sarvam AI may not have a dedicated embeddings endpoint.
        This implementation uses a workaround by leveraging the chat/completions
        endpoint to generate vector representations, or uses a third-party service.
        
        Args:
            texts (List[str]): List of text strings to embed
            max_retries (int): Maximum number of retry attempts
            
        Returns:
            Optional[List[List[float]]]: List of embedding vectors
        """
        # Since Sarvam AI doesn't have a public embeddings API yet,
        # we'll create a simple embedding using the text's characteristics
        # This is a placeholder implementation that should be replaced
        # with actual Sarvam embeddings API when available
        
        logger.warning("Sarvam AI embeddings API not yet available. Using fallback embedding method.")
        
        embeddings = []
        for text in texts:
            try:
                # Create a simple embedding based on text characteristics
                # This is a very basic implementation and should be replaced
                # with actual API calls when Sarvam provides embeddings
                embedding = self._create_simple_embedding(text)
                embeddings.append(embedding)
            except Exception as e:
                logger.error(f"Error generating embedding: {str(e)}")
                # Return a zero vector on error
                embeddings.append([0.0] * 384)  # 384-dimensional vector
        
        return embeddings
    
    def _create_simple_embedding(self, text: str, dimension: int = 384) -> List[float]:
        """
        Create a simple embedding vector based on text characteristics.
        This is a placeholder until Sarvam provides embeddings API.
        
        Args:
            text (str): Text to embed
            dimension (int): Dimension of embedding vector
            
        Returns:
            List[float]: Embedding vector
        """
        import hashlib
        
        # Create a hash of the text
        text_hash = hashlib.sha256(text.encode()).hexdigest()
        
        # Convert hash to numbers and normalize
        embedding = []
        for i in range(dimension):
            # Use parts of the hash to generate pseudo-random numbers
            hash_part = text_hash[(i * 2) % len(text_hash):(i * 2 + 2) % len(text_hash)]
            value = int(hash_part or "00", 16) / 255.0 * 2 - 1  # Normalize to [-1, 1]
            embedding.append(value)
        
        return embedding
    
    def analyze_email(self, email_content: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        Analyze email content using Sarvam's chat/completions endpoint.
        
        Args:
            email_content (str): Email content to analyze
            max_retries (int): Maximum number of retry attempts
            
        Returns:
            Optional[Dict[str, Any]]: Analysis results including summary, key points, etc.
        """
        endpoint = f"{self.base_url}/chat/completions"
        
        # Create prompt for email analysis
        prompt = f"""Analyze the following email and provide:
1. A brief summary (2-3 sentences)
2. Key points (bullet points)
3. Sentiment (positive/negative/neutral)
4. Category (work/personal/spam/marketing/etc)
5. Action items (if any)

Email:
{email_content[:2000]}  # Limit content length

Provide the response in JSON format with keys: summary, key_points, sentiment, category, action_items"""
        
        payload = {
            "model": "sarvam-1",  # Default model, adjust as needed
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.3,
            "max_tokens": 500
        }
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Analyzing email with Sarvam API (attempt {attempt + 1}/{max_retries})")
                response = requests.post(
                    endpoint,
                    headers=self.headers,
                    json=payload,
                    timeout=30
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    # Extract the analysis from the response
                    if "choices" in result and len(result["choices"]) > 0:
                        content = result["choices"][0].get("message", {}).get("content", "")
                        
                        # Try to parse as JSON
                        try:
                            analysis = json.loads(content)
                        except json.JSONDecodeError:
                            # If not JSON, create structured response
                            analysis = {
                                "summary": content[:200],
                                "key_points": [],
                                "sentiment": "neutral",
                                "category": "uncategorized",
                                "action_items": []
                            }
                        
                        logger.info("Successfully analyzed email")
                        return analysis
                    else:
                        logger.warning("No choices in response")
                        return self._get_default_analysis()
                
                elif response.status_code == 429:
                    # Rate limit exceeded
                    logger.warning(f"Rate limit exceeded (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep(5)  # Wait longer for rate limits
                        continue
                    return self._get_default_analysis()
                
                else:
                    logger.error(f"API error: {response.status_code} - {response.text}")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    return self._get_default_analysis()
                    
            except requests.exceptions.Timeout:
                logger.error(f"Request timeout (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return self._get_default_analysis()
                
            except Exception as e:
                logger.error(f"Error analyzing email (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return self._get_default_analysis()
        
        return self._get_default_analysis()
    
    def _get_default_analysis(self) -> Dict[str, Any]:
        """Return default analysis structure when API fails."""
        return {
            "summary": "Email analysis not available",
            "key_points": [],
            "sentiment": "neutral",
            "category": "uncategorized",
            "action_items": [],
            "error": "Failed to analyze email"
        }
    
    def analyze_batch(self, email_contents: List[str], batch_size: int = 5) -> List[Dict[str, Any]]:
        """
        Analyze multiple emails in batches.
        
        Args:
            email_contents (List[str]): List of email contents to analyze
            batch_size (int): Number of emails to process at once
            
        Returns:
            List[Dict[str, Any]]: List of analysis results
        """
        results = []
        
        for i in range(0, len(email_contents), batch_size):
            batch = email_contents[i:i + batch_size]
            
            for content in batch:
                analysis = self.analyze_email(content)
                results.append(analysis)
                
                # Small delay between requests to avoid rate limiting
                time.sleep(0.5)
        
        return results
    
    def test_connection(self) -> bool:
        """
        Test the connection to Sarvam API.
        
        Returns:
            bool: True if connection is successful
        """
        try:
            # Try a simple completion request
            endpoint = f"{self.base_url}/chat/completions"
            payload = {
                "model": "sarvam-1",
                "messages": [{"role": "user", "content": "Hello"}],
                "max_tokens": 10
            }
            
            response = requests.post(
                endpoint,
                headers=self.headers,
                json=payload,
                timeout=10
            )
            
            if response.status_code in [200, 400, 401]:  # 401/400 means auth issue but API is reachable
                logger.info("Sarvam API connection test successful")
                return True
            else:
                logger.error(f"Sarvam API connection test failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error testing Sarvam API connection: {str(e)}")
            return False

