using System;

namespace Riak.Driver
{
    #region DeleteOptions
    /// <summary>
    /// riak delete object options
    /// </summary>
    public sealed class DeleteOptions
    {
        private readonly Messages.RpbDelReq _req = null;

        /// <summary>
        /// new
        /// </summary>
        /// <param name="req"></param>
        /// <exception cref="ArgumentNullException">req is null</exception>
        public DeleteOptions(Messages.RpbDelReq req)
        {
            if (req == null) throw new ArgumentNullException("req");
            this._req = req;
        }

        /// <summary>
        /// set rw
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public DeleteOptions SetRW(uint value)
        {
            this._req.rw = value;
            return this;
        }
        /// <summary>
        /// set r
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public DeleteOptions SetR(uint value)
        {
            this._req.r = value;
            return this;
        }
        /// <summary>
        /// set w
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public DeleteOptions SetW(uint value)
        {
            this._req.w = value;
            return this;
        }
        /// <summary>
        /// set pr
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public DeleteOptions SetPR(uint value)
        {
            this._req.pr = value;
            return this;
        }
        /// <summary>
        /// set pw
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public DeleteOptions SetPW(uint value)
        {
            this._req.pw = value;
            return this;
        }
        /// <summary>
        /// set dw
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public DeleteOptions SetDW(uint value)
        {
            this._req.dw = value;
            return this;
        }
    }
    #endregion

    #region GetOptions
    /// <summary>
    /// riak get object options
    /// </summary>
    public sealed class GetOptions
    {
        private readonly Messages.RpbGetReq _req = null;

        /// <summary>
        /// new
        /// </summary>
        /// <param name="req"></param>
        /// <exception cref="ArgumentNullException">req is null</exception>
        public GetOptions(Messages.RpbGetReq req)
        {
            if (req == null) throw new ArgumentNullException("req");
            this._req = req;
        }

        /// <summary>
        /// set r
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public GetOptions SetR(uint value)
        {
            this._req.r = value;
            return this;
        }
        /// <summary>
        /// set dw
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public GetOptions SetPR(uint value)
        {
            this._req.pr = value;
            return this;
        }
    }
    #endregion

    #region PutOptions
    /// <summary>
    /// riak put object options
    /// </summary>
    public sealed class PutOptions
    {
        private readonly Messages.RpbPutReq _req = null;

        /// <summary>
        /// new
        /// </summary>
        /// <param name="req"></param>
        /// <exception cref="ArgumentNullException">req is null</exception>
        public PutOptions(Messages.RpbPutReq req)
        {
            if (req == null) throw new ArgumentNullException("req");
            this._req = req;
        }

        /// <summary>
        /// set w
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public PutOptions SetW(uint value)
        {
            this._req.w = value;
            return this;
        }
        /// <summary>
        /// set dw
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public PutOptions SetDW(uint value)
        {
            this._req.dw = value;
            return this;
        }
        /// <summary>
        /// set pw
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public PutOptions SetPW(uint value)
        {
            this._req.pw = value;
            return this;
        }
        /// <summary>
        /// set return body
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public PutOptions SetReturnBody(bool value)
        {
            this._req.return_body = value;
            return this;
        }
    }
    #endregion

    #region CounterUpdateOptions
    /// <summary>
    /// riak counter update options
    /// </summary>
    public sealed class CounterUpdateOptions
    {
        private readonly Messages.RpbCounterUpdateReq _req = null;

        /// <summary>
        /// new
        /// </summary>
        /// <param name="req"></param>
        /// <exception cref="ArgumentNullException">req is null</exception>
        public CounterUpdateOptions(Messages.RpbCounterUpdateReq req)
        {
            if (req == null) throw new ArgumentNullException("req");
            this._req = req;
        }

        /// <summary>
        /// set w
        /// </summary>
        /// <param name="value"></param>
        public CounterUpdateOptions SetW(uint value)
        {
            this._req.w = value;
            return this;
        }
        /// <summary>
        /// set pw
        /// </summary>
        /// <param name="value"></param>
        public CounterUpdateOptions SetPW(uint value)
        {
            this._req.pw = value;
            return this;
        }
        /// <summary>
        /// set dw
        /// </summary>
        /// <param name="value"></param>
        public CounterUpdateOptions SetDW(uint value)
        {
            this._req.dw = value;
            return this;
        }
        /// <summary>
        /// set return value
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public CounterUpdateOptions SetReturnValue(bool value)
        {
            this._req.returnvalue = value;
            return this;
        }
    }
    #endregion

    #region CounterGetOptions
    /// <summary>
    /// riak counter get options
    /// </summary>
    public sealed class CounterGetOptions
    {
        private readonly Messages.RpbCounterGetReq _req = null;

        /// <summary>
        /// new
        /// </summary>
        /// <param name="req"></param>
        /// <exception cref="ArgumentNullException">req is null</exception>
        public CounterGetOptions(Messages.RpbCounterGetReq req)
        {
            if (req == null) throw new ArgumentNullException("req");
            this._req = req;
        }

        /// <summary>
        /// set r
        /// </summary>
        /// <param name="value"></param>
        public CounterGetOptions SetR(uint value)
        {
            this._req.r = value;
            return this;
        }
        /// <summary>
        /// set pr
        /// </summary>
        /// <param name="value"></param>
        public CounterGetOptions SetPR(uint value)
        {
            this._req.pr = value;
            return this;
        }
    }
    #endregion
}