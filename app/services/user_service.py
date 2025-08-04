from typing import Optional
from bson import ObjectId
from ..models.user import UserModel, UserInDB
from ..core.database import get_collection
from ..core.security import get_password_hash, verify_password
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class UserService:
    def __init__(self):
        pass  # Do not assign collection here
    
    async def create_user(self, user_data: dict) -> Optional[UserInDB]:
        """Create a new user."""
        try:
            collection = get_collection("users")
            # Check if user already exists
            existing_user = await self.get_user_by_email(user_data["email"])
            if existing_user:
                return None
            
            # Hash password
            user_data["hashed_password"] = get_password_hash(user_data["password"])
            del user_data["password"]
            
            # Create user document
            user_doc = UserModel(**user_data)
            result = await collection.insert_one(user_doc.dict(by_alias=True))
            
            if result.inserted_id:
                created_user = await self.get_user_by_id(str(result.inserted_id))
                return created_user
            
            return None
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            return None
    
    async def get_user_by_email(self, email: str) -> Optional[UserInDB]:
        """Get user by email."""
        try:
            collection = get_collection("users")
            user_doc = await collection.find_one({"email": email})
            if user_doc:
                return UserInDB(**user_doc)
            return None
        except Exception as e:
            logger.error(f"Error getting user by email: {e}")
            return None
    
    async def get_user_by_id(self, user_id: str) -> Optional[UserInDB]:
        """Get user by ID."""
        try:
            collection = get_collection("users")
            user_doc = await collection.find_one({"_id": ObjectId(user_id)})
            if user_doc:
                return UserInDB(**user_doc)
            return None
        except Exception as e:
            logger.error(f"Error getting user by ID: {e}")
            return None
    
    async def authenticate_user(self, email: str, password: str) -> Optional[UserInDB]:
        """Authenticate user with email and password."""
        try:
            user = await self.get_user_by_email(email)
            if not user:
                return None
            
            if not verify_password(password, user.hashed_password):
                return None
            
            return user
        except Exception as e:
            logger.error(f"Error authenticating user: {e}")
            return None
    
    async def update_user(self, user_id: str, update_data: dict) -> Optional[UserInDB]:
        """Update user information."""
        try:
            collection = get_collection("users")
            # Remove password from update data if present
            if "password" in update_data:
                update_data["hashed_password"] = get_password_hash(update_data["password"])
                del update_data["password"]
            
            update_data["updated_at"] = datetime.utcnow()
            
            result = await collection.update_one(
                {"_id": ObjectId(user_id)},
                {"$set": update_data}
            )
            
            if result.modified_count:
                return await self.get_user_by_id(user_id)
            
            return None
        except Exception as e:
            logger.error(f"Error updating user: {e}")
            return None


# Create service instance
user_service = UserService() 