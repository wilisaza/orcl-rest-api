import CryptoJS from "crypto-js"
import { isEmpty } from "../utils/index.js"

export const decodeString = (decodeKey, value) => {
  if(isEmpty(decodeKey)) 
    return value;

  const decodeValue = CryptoJS.AES.decrypt(value, decodeKey);
  return decodeValue.toString(CryptoJS.enc.Utf8);
}
